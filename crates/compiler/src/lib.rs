//! FlowLog Compiler Library
//! Generates executable Rust code from planned strata.

// =========================================================================
// Module Declarations
// =========================================================================
mod aggregation;
mod arg;
mod comm;
mod data_type;
mod flow;
mod fs_utils;
mod ident;
mod import;
mod inspect;
mod read;
mod relation;
mod scaffold;
mod transformation;

// =========================================================================
// Imports
// =========================================================================
use import::ImportTracker;
use proc_macro2::{Ident, TokenStream};
use quote::quote;
use std::collections::{HashMap, HashSet};
use syn::{parse2, File};

use common::{Config, ExecutionMode};
use parser::{DataType, Program};
use planner::StratumPlanner;

use inspect::gen_delete_partitions;

pub struct Compiler {
    /// Configuration provided to the compiler.
    config: Config,

    /// The parsed FlowLog program.
    program: Program,

    /// Global map from relation fingerprint to its identifier.
    global_fp_to_ident: HashMap<u64, Ident>,

    /// Global map from relation fingerprint to its key-value data type.
    global_fp_to_type: HashMap<u64, (Vec<DataType>, Vec<DataType>)>,

    /// Tracker for imports needed.
    imports: ImportTracker,
}

impl Compiler {
    /// Create a new Compiler instance from Config and Program.
    pub fn new(config: Config, program: Program) -> Self {
        let mut compiler = Compiler {
            config,
            program,
            global_fp_to_ident: HashMap::new(),
            global_fp_to_type: HashMap::new(),
            imports: ImportTracker::default(),
        };
        compiler.make_global_ident_map();
        compiler.make_global_type_map();

        compiler
    }

    /// Create executable from the strata plan.
    pub fn generate_executable_at(&mut self, strata: &[StratumPlanner]) -> std::io::Result<()> {
        let main = self.generate_main(strata);
        self.write_project(&main)
    }
}

// =========================================================================
// Project Generation Utilities
// =========================================================================
impl Compiler {
    /// Generate the text for a standalone `main.rs` program executing the provided strata.
    fn generate_main(&mut self, strata: &[StratumPlanner]) -> String {
        self.imports.reset(self.config.mode());

        // Static sections of the generated program.
        let input_decls = self.gen_input_decls();
        let (lhs_binding, ret_expr) = self.build_handle_binding();

        // Flow generation per stratum.
        let mut flow_stmts: Vec<TokenStream> = Vec::new();
        let mut calculated_output_fps: HashSet<u64> = HashSet::new();

        for stratum in strata {
            let (core_flows, non_recursive_arranged_map) =
                self.gen_non_recursive_core_flows(stratum.non_recursive_transformations());
            flow_stmts.extend(core_flows);

            if stratum.is_recursive() {
                flow_stmts.push(self.gen_iterative_block(&non_recursive_arranged_map, stratum));
            } else {
                flow_stmts
                    .extend(self.gen_non_recursive_post_flows(&calculated_output_fps, stratum));
            }

            calculated_output_fps.extend(stratum.output_relations());
        }

        let (inspect_stmts, merge_stmts, delete_stmts) = self.collect_inspectors();

        // Imports block (conditional on recursion for Variable).
        let imports = self.imports.render();

        let timestamp_type = match self.config.mode() {
            ExecutionMode::Incremental => quote! { u32 },
            ExecutionMode::Batch => quote! { () },
        };

        // --- incremental: generate rel registry inserts from EDB list ---
        // Assumptions (match your current generated code):
        //   - input handle idents are named h{rel_name}, e.g., hsource, harc
        //   - rel ops concrete types are {CamelCase(rel_name)}Rel, e.g., SourceRel, ArcRel
        let rel_build_stmts: Vec<TokenStream> = if matches!(
            self.config.mode(),
            ExecutionMode::Incremental
        ) {
            self.program
                .edbs()
                .iter()
                .map(|edb| {
                    let rel_name = edb.name().to_ascii_lowercase();

                    let handle_ident =
                        Ident::new(&format!("h{rel_name}"), proc_macro2::Span::call_site());

                    let ops_ty_ident = Ident::new(
                        &format!("Rel{}", &rel_name),
                        proc_macro2::Span::call_site(),
                    );

                    quote! {
                        rels.insert(#rel_name.to_string(), Box::new(#ops_ty_ident::new(#handle_ident)));
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        // --- generate the whole fn main() depending on mode (minimal but correct) ---
        let main_fn = match self.config.mode() {
            ExecutionMode::Batch => {
                let ingest_stmts = self.gen_ingest_stmts();
                let close_stmts = self.gen_close_stmts();

                quote! {
                    fn main() {
                        timely::execute_from_args(std::env::args(), |worker| {
                            // --- Runtime setup -------------------------------------------------
                            let timer = Instant::now();
                            let peers = worker.peers();
                            let index = worker.index();

                            // --- Build dataflow graph -----------------------------------------
                            let #lhs_binding =
                                worker.dataflow::<#timestamp_type, _, _>(|scope| {
                                    #(#input_decls)*

                                    // === Transformation flows ===
                                    #(#flow_stmts)*

                                    // === Inspect IDB sizes ===
                                    #(#inspect_stmts)*

                                    #ret_expr
                                });

                            if index == 0 {
                                println!("{:?}:\tDataflow assembled", timer.elapsed());
                            }

                            // --- Data ingestion -----------------------------------------------
                            #(#ingest_stmts)*
                            #(#close_stmts)*

                            // --- Execute to fixpoint -------------------------------------------
                            while worker.step() {}

                            if index == 0 {
                                println!("{:?}:\tDataflow executed", timer.elapsed());
                                // === Merge per-worker output partitions (if any) ===
                                #(#merge_stmts)*
                            }
                        }).unwrap();
                    }
                }
            }

            ExecutionMode::Incremental => quote! {
                // -------------------------------
                // Worker sync helpers
                // -------------------------------
                fn workers_from_args(args: &[String]) -> usize {
                    let mut i = 0;
                    while i < args.len() {
                        if args[i] == "-w" && i + 1 < args.len() {
                            if let Ok(n) = args[i + 1].parse::<usize>() {
                                return n.max(1);
                            }
                            i += 2;
                            continue;
                        }
                        if let Some(rest) = args[i].strip_prefix("-w=") {
                            if let Ok(n) = rest.parse::<usize>() {
                                return n.max(1);
                            }
                        }
                        i += 1;
                    }
                    1
                }

                fn main() {
                    let args_vec: Vec<String> = std::env::args().collect();
                    let workers = workers_from_args(&args_vec);

                    let shared_txn: Arc<RwLock<TxnState>> = Arc::new(RwLock::new(TxnState::default()));
                    let barrier = Arc::new(Barrier::new(workers));

                    timely::execute_from_args(std::env::args(), {
                        let shared_txn = shared_txn.clone();
                        let barrier = barrier.clone();

                        move |worker| {
                            // --- Runtime setup -------------------------------------------------
                            let timer = Instant::now();
                            let peers = worker.peers();
                            let index = worker.index();

                            // --- Build dataflow graph -----------------------------------------
                            let #lhs_binding =
                                worker.dataflow::<#timestamp_type, _, _>(|scope| {
                                    #(#input_decls)*

                                    // === Transformation flows ===
                                    #(#flow_stmts)*

                                    // === Probe ===
                                    let mut probe = ProbeHandle::new();

                                    // === Inspect IDB sizes / outputs ===
                                    #(#inspect_stmts)*

                                    #ret_expr
                                });

                            // --- Build rel registry (EDBs) ------------------------------------
                            let mut rels: HashMap<String, Box<dyn RelOps>> = HashMap::new();
                            #(#rel_build_stmts)*

                            // Helper: apply a list of txn ops to this worker's input handles.
                            fn apply_ops(
                                rels: &mut HashMap<String, Box<dyn RelOps>>,
                                ops: &[TxnOp],
                                peers: usize,
                                index: usize,
                            ) {
                                for op in ops {
                                    match op {
                                        TxnOp::Put { rel, tuple, diff } => {
                                            let r = rels
                                                .get_mut(&rel.to_ascii_lowercase())
                                                .unwrap_or_else(|| panic!("unknown relation: '{rel}'"));
                                            r.apply_tuple(tuple, *diff, peers, index);
                                        }
                                        TxnOp::File { rel, path, diff } => {
                                            let r = rels
                                                .get_mut(&rel.to_ascii_lowercase())
                                                .unwrap_or_else(|| panic!("unknown relation: '{rel}'"));
                                            r.apply_file(path.as_path(), *diff, peers, index);
                                        }
                                    }
                                }
                            }

                            if index == 0 {
                                println!("{:?}:\tDataflow assembled", timer.elapsed());
                                println!("FlowLog Incremental Interactive Shell, type 'help' for commands.");
                            }

                            let mut time_stamp: u32 = 0;
                            let mut last_epoch_seen: u32 = 0;

                            // -------------------------------
                            // Worker != 0: listen & apply published txn snapshots
                            // -------------------------------
                            if index != 0 {
                                loop {
                                    barrier.wait();

                                    let snap = shared_txn.read().unwrap().clone();
                                    assert!(snap.epoch > last_epoch_seen, "stale epoch observed");
                                    last_epoch_seen = snap.epoch;

                                    let mut should_quit = false;

                                    match snap.action {
                                        TxnAction::Commit => {
                                            apply_ops(&mut rels, snap.pending.as_slice(), peers, index);

                                            time_stamp += 1;
                                            for r in rels.values_mut() {
                                                r.advance_to(time_stamp);
                                                r.flush();
                                            }
                                            while probe.less_than(&time_stamp) {
                                                worker.step();
                                            }

                                            barrier.wait();
                                        }
                                        TxnAction::Quit => {
                                            for r in rels.values_mut() {
                                                r.close();
                                            }
                                            while probe.less_than(&time_stamp) {
                                                worker.step();
                                            }

                                            barrier.wait();
                                            should_quit = true;
                                        }
                                        TxnAction::None => {
                                            barrier.wait();
                                        }
                                    }

                                    barrier.wait();

                                    if should_quit {
                                        break;
                                    }
                                }
                                return;
                            }

                            // -------------------------------
                            // Worker 0: interactive driver
                            // -------------------------------
                            let rel_words = rels.keys().cloned().collect::<Vec<_>>();
                            let mut prompt = Prompt::new(rel_words);

                            let mut local_txn: TxnState = TxnState::default();

                            loop {
                                let Some(c) = prompt.next_cmd() else { continue };

                                match c {
                                    Cmd::Help => println!("{}", cmd::help_text()),

                                    Cmd::Begin => {
                                        local_txn.begin();
                                        println!("(txn begin)");
                                    }

                                    Cmd::Abort => {
                                        local_txn.abort();
                                        println!("(txn aborted)");
                                    }

                                    Cmd::Put { rel, tuple, diff } => {
                                        if !local_txn.in_txn {
                                            local_txn.begin();
                                        }
                                        local_txn.enqueue(TxnOp::Put { rel, tuple, diff });
                                        println!("(queued put)");
                                    }

                                    Cmd::File { rel, path, diff } => {
                                        if !local_txn.in_txn {
                                            local_txn.begin();
                                        }
                                        local_txn.enqueue(TxnOp::File { rel, path, diff });
                                        println!("(queued file)");
                                    }

                                    Cmd::Commit => {
                                        if !local_txn.in_txn {
                                            println!("(no active txn)");
                                            continue;
                                        }

                                        let round_timer = Instant::now();

                                        let next_epoch = shared_txn.read().unwrap().epoch + 1;
                                        {
                                            let mut w = shared_txn.write().unwrap();
                                            *w = local_txn.as_commit_snapshot(next_epoch);
                                        }

                                        barrier.wait();

                                        // Apply exactly what got published (keeps behavior consistent).
                                        let snap = shared_txn.read().unwrap().clone();
                                        apply_ops(&mut rels, snap.pending.as_slice(), peers, index);

                                        time_stamp += 1;
                                        for r in rels.values_mut() {
                                            r.advance_to(time_stamp);
                                            r.flush();
                                        }
                                        while probe.less_than(&time_stamp) {
                                            worker.step();
                                        }

                                        barrier.wait();

                                        // === Merge per-worker output partitions (if any) ===
                                        #(#merge_stmts)*

                                        println!("{:?}:\tCommitted & executed", round_timer.elapsed());

                                        local_txn.abort();

                                        barrier.wait();

                                        {
                                            let mut w = shared_txn.write().unwrap();
                                            w.action = TxnAction::None;
                                            w.pending.clear();
                                            w.in_txn = false;
                                        }
                                    }

                                    Cmd::Quit => {
                                        let next_epoch = shared_txn.read().unwrap().epoch + 1;
                                        {
                                            let mut w = shared_txn.write().unwrap();
                                            *w = TxnState::as_quit_snapshot(next_epoch);
                                        }

                                        barrier.wait();

                                        for r in rels.values_mut() {
                                            r.close();
                                        }
                                        while probe.less_than(&time_stamp) {
                                            worker.step();
                                        }

                                        barrier.wait();

                                        // === incremental quit: clean up tmp per-worker partition files ===
                                        #(#delete_stmts)*

                                        barrier.wait();
                                        break;
                                    }
                                }
                            }
                        }
                    }).unwrap();
                }
            },
        };

        let file_ts: TokenStream = quote! {
            #imports
            #main_fn
        };

        let ast: File = parse2(file_ts).expect("valid token stream");
        prettyplease::unparse(&ast)
    }

    /// Assemble inspection and partition-merge statements for IDB relations based on CLI args.
    fn collect_inspectors(&mut self) -> (Vec<TokenStream>, Vec<TokenStream>, Vec<TokenStream>) {
        let mut inspect_stmts = Vec::new();
        let mut merge_stmts = Vec::new();
        let mut delete_stmts = Vec::new();

        for idb in self.program.idbs() {
            let var = self.find_global_ident(idb.fingerprint());
            let name = idb.name();

            if idb.printsize() {
                self.imports.mark_threshold_total();
                self.imports.mark_as_collection();
                self.imports.mark_timely_map();
                self.imports.mark_semiring_one();
                inspect_stmts.push(self.gen_size_inspector(&var, name));
            }

            if idb.output() {
                if self.config.output_to_stdout() {
                    inspect_stmts.push(self.gen_print_inspector(&var, name, idb.arity()));
                } else {
                    let parent_dir = self.config.output_dir().expect(
                        "output directory must be provided when writing IDB output to files",
                    );
                    inspect_stmts.push(self.gen_write_inspector(
                        &var,
                        name,
                        parent_dir,
                        idb.arity(),
                    ));
                    merge_stmts.push(self.gen_merge_partitions(name, parent_dir));

                    if self.config.is_incremental() {
                        delete_stmts.push(gen_delete_partitions(name, parent_dir));
                    }
                }
            }
        }

        (inspect_stmts, merge_stmts, delete_stmts)
    }
}
