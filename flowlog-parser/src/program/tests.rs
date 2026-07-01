use std::fs;
use std::io::Write;

use flowlog_common::SourceMap;
use tempfile::NamedTempFile;
use tempfile::tempdir;

use super::*;
use crate::ComparisonOperator;
use crate::DataType;
use crate::HeadArg;
use crate::error::ParseError;
use crate::logic::LoopBlock;
use crate::logic::Predicate;

fn loop_blocks(program: &Program) -> Vec<&LoopBlock> {
    program
        .segments()
        .iter()
        .filter_map(|s| s.as_loop())
        .collect()
}

fn parse_program(src: &str) -> Program {
    parse_program_result(src).expect("parse failed")
}

fn parse_program_result(src: &str) -> Result<Program, ParseError> {
    let mut tmp = NamedTempFile::new().expect("failed to create temp file");
    tmp.write_all(src.as_bytes())
        .expect("failed to write temp file");
    let mut sm = SourceMap::new();
    Program::parse(&tmp.path().to_string_lossy(), true, &[], &mut sm)
}

fn find_relation<'a>(program: &'a Program, name: &str) -> &'a Relation {
    program
        .relations()
        .iter()
        .find(|r| r.name() == name)
        .unwrap_or_else(|| panic!("relation `{name}` not found"))
}

#[test]
fn decl_case_collision_rejected() {
    let err = parse_program_result(
        "
        .decl edge(x: number)
        .decl Edge(y: number)
        ",
    )
    .unwrap_err();
    assert!(
        matches!(err, ParseError::DuplicateDecl { .. }),
        "got {err:?}"
    );
}

#[test]
fn attr_case_collision_rejected() {
    let err = parse_program_result(".decl edge(x: number, X: number)").unwrap_err();
    assert!(
        matches!(err, ParseError::DuplicateAttribute { .. }),
        "got {err:?}"
    );
}

#[test]
fn duplicate_extern_fn_rejected() {
    let err = parse_program_result(
        "
        .extern fn f(x: int64) -> int64
        .extern fn f(y: int64) -> int64
        ",
    )
    .unwrap_err();
    assert!(
        matches!(err, ParseError::DuplicateExternFn { .. }),
        "got {err:?}"
    );
}

#[test]
fn ordering_preserved() {
    let src = "
        .decl a(x: number)
        .decl b(x: number)
        .decl c(x: number)
        .output c
        a(X) :- b(X).
        fixpoint { b(X) :- a(X). }
        c(X) :- a(X).
    ";
    let program = parse_program(src);
    let items = program.segments();
    assert_eq!(items.len(), 3);
    assert!(matches!(&items[0], Segment::Plain(r) if r.len() == 1));
    assert!(matches!(&items[1], Segment::Fixpoint(_)));
    assert!(matches!(&items[2], Segment::Plain(r) if r.len() == 1));
}

#[test]
fn multiple_rules_before_loop_form_one_segment() {
    let src = "
        .decl a(x: number)
        .decl b(x: number)
        .output a
        a(1) :- b(1).
        a(2) :- b(2).
        fixpoint { a(X) :- b(X). }
    ";
    let program = parse_program(src);
    let items = program.segments();
    // Two rules collapse into one segment, then the fixpoint.
    assert_eq!(items.len(), 2);
    assert!(matches!(&items[0], Segment::Plain(r) if r.len() == 2));
    assert!(matches!(&items[1], Segment::Fixpoint(_)));
}

#[test]
fn rules_method_flattens_segments() {
    let src = "
        .decl a(x: number)
        .decl b(x: number)
        .output a
        a(X) :- b(X).
        fixpoint { }
        a(1) :- b(1).
    ";
    let program = parse_program(src);
    // rules() flattens top-level rules only (excludes loop internals).
    assert_eq!(program.rules().len(), 2);
}

#[test]
fn loop_block_with_declared_relation() {
    let src = "
        .decl done()
        .decl edge(x: number, y: number)
        .output done
        loop until { done } { done() :- edge(1, 2). }
    ";
    let program = parse_program(src);
    assert_eq!(loop_blocks(&program).len(), 1);
}

#[test]
fn fixpoint_needs_no_declaration() {
    let src = "
        .decl edge(x: number, y: number)
        .output edge
        fixpoint { edge(1, 2) :- edge(1, 2). }
    ";
    let program = parse_program(src);
    assert_eq!(loop_blocks(&program).len(), 1);
}

#[test]
fn loop_while_needs_no_declaration() {
    let src = "
        .decl edge(x: number, y: number)
        .output edge
        loop while { @it <= 9 } { edge(1, 2) :- edge(1, 2). }
    ";
    let program = parse_program(src);
    assert_eq!(loop_blocks(&program).len(), 1);
}

#[test]
fn iterative_declared_passes() {
    let src = "
        .decl edge(x: number, y: number)
        .decl active_edge(x: number, y: number)
        .output active_edge
        fixpoint { .iterative active_edge  active_edge(X, Y) :- edge(X, Y). }
    ";
    let program = parse_program(src);
    assert_eq!(loop_blocks(&program).len(), 1);
    assert_eq!(loop_blocks(&program)[0].iterative_relations().len(), 1);
}

#[test]
fn dead_code_elimination_keeps_loop_until_relations() {
    let src = "
        .decl edge(x: number, y: number)
        .decl keep()
        .decl dead()
        .output edge

        edge(1, 2).

        loop until { keep } {
            keep() :- edge(1, 2).
        }

        dead() :- edge(2, 3).
    ";
    let program = parse_program(src);

    assert!(program.relations().iter().any(|rel| rel.name() == "keep"));
    assert!(!program.relations().iter().any(|rel| rel.name() == "dead"));
}

#[test]
fn edb_subsets_track_file_backed_inline_and_overlap_relations() {
    let src = "
        .decl file_only(x: number)
        .decl fact_only(x: number)
        .decl both(x: number)
        .decl out(x: number)
        .input file_only(IO=\"file\", filename=\"file_only.csv\", delimiter=\",\")
        .input both(IO=\"file\", filename=\"both.csv\", delimiter=\",\")
        .output out

        fact_only(1).
        both(2).

        out(X) :- file_only(X).
        out(X) :- fact_only(X).
        out(X) :- both(X).
    ";
    let program = parse_program(src);

    let mut edbs = program
        .edbs()
        .into_iter()
        .map(|rel| rel.name().to_string())
        .collect::<Vec<_>>();
    edbs.sort_unstable();

    let mut file_backed = program
        .file_backed_relations()
        .into_iter()
        .map(|rel| rel.name().to_string())
        .collect::<Vec<_>>();
    file_backed.sort_unstable();

    let mut inline_facts = program
        .inline_fact_relations()
        .into_iter()
        .map(|rel| rel.name().to_string())
        .collect::<Vec<_>>();
    inline_facts.sort_unstable();

    assert_eq!(edbs, vec!["both", "fact_only", "file_only"]);
    assert_eq!(file_backed, vec!["both", "file_only"]);
    assert_eq!(inline_facts, vec!["both", "fact_only"]);
}

/// `.input Edge` with no parens / no params is accepted and
/// resolves to Soufflé defaults: file-backed, `Edge.facts`, TAB.
#[test]
fn bare_input_directive_uses_souffle_defaults() {
    let src = "
        .decl Edge(a: symbol, b: symbol)
        .input Edge
    ";
    let program = parse_program(src);
    let edge = find_relation(&program, "edge");
    assert!(edge.has_input(), "bare .input attaches params");
    assert!(edge.is_file_backed(), "absent IO= defaults to file");
    assert_eq!(edge.input_file_name(), "Edge.facts");
    assert_eq!(edge.input_delimiter(), "\t");
}

#[test]
fn multi_head_rule_expands() {
    let src = "
        .decl a(x: number)
        .decl b(x: number)
        .decl c(x: number)
        .output b
        .output c
        b(X), c(X) :- a(X).
    ";
    let program = parse_program(src);
    let rules = program.rules();
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].head().name(), "b");
    assert_eq!(rules[1].head().name(), "c");
    // Both share the same body
    assert_eq!(rules[0].rhs().len(), 1);
    assert_eq!(rules[1].rhs().len(), 1);
}

#[test]
fn multi_head_rule_in_fixpoint() {
    let src = "
        .decl a(x: number, y: number)
        .decl b(x: number, y: number)
        .decl c(x: number, y: number)
        .output b
        .output c
        fixpoint { b(X, Y), c(X, Y) :- a(X, Y). }
    ";
    let program = parse_program(src);
    let blocks = loop_blocks(&program);
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].rules().len(), 2);
    assert_eq!(blocks[0].rules()[0].head().name(), "b");
    assert_eq!(blocks[0].rules()[1].head().name(), "c");
}

#[test]
fn multi_body_rule_expands() {
    let src = "
        .decl a(x: number)
        .decl b(x: number)
        .decl c(x: number)
        .output c
        c(X) :- a(X); b(X).
    ";
    let program = parse_program(src);
    let rules = program.rules();
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].head().name(), "c");
    assert_eq!(rules[1].head().name(), "c");
    assert_eq!(rules[0].rhs()[0].name(), "a");
    assert_eq!(rules[1].rhs()[0].name(), "b");
}

#[test]
fn disjunction_arm_can_be_a_conjunction() {
    // `(A, B ; C, D)` means `(A AND B) OR (C AND D)` — each arm
    // of a `;` may itself be a comma-separated conjunction.
    let src = "
        .decl a(x: number) .decl b(x: number)
        .decl c(x: number) .decl d(x: number)
        .decl r(x: number)
        .output r
        r(X) :- ( a(X), b(X) ; c(X), d(X) ).
    ";
    let program = parse_program(src);
    let rules = program.rules();
    assert_eq!(rules.len(), 2);
    let bodies: Vec<Vec<&str>> = rules
        .iter()
        .map(|r| r.rhs().iter().map(|p| p.name()).collect())
        .collect();
    assert!(bodies.contains(&vec!["a", "b"]));
    assert!(bodies.contains(&vec!["c", "d"]));
}

#[test]
fn nested_disjunctions_cross_product() {
    let src = "
        .decl a(x: number)
        .decl b(x: number)
        .decl c(x: number)
        .decl d(x: number)
        .decl r(x: number)
        .output r
        r(X) :- ( a(X) ; b(X) ), ( c(X) ; d(X) ).
    ";
    let program = parse_program(src);
    let rules = program.rules();
    assert_eq!(rules.len(), 4);
    let bodies: Vec<(&str, &str)> = rules
        .iter()
        .map(|r| (r.rhs()[0].name(), r.rhs()[1].name()))
        .collect();
    assert!(bodies.contains(&("a", "c")));
    assert!(bodies.contains(&("a", "d")));
    assert!(bodies.contains(&("b", "c")));
    assert!(bodies.contains(&("b", "d")));
}

#[test]
fn multi_head_multi_body_expands() {
    let src = "
        .decl a(x: number)
        .decl b(x: number)
        .decl c(x: number)
        .decl d(x: number)
        .output c
        .output d
        c(X), d(X) :- a(X); b(X).
    ";
    let program = parse_program(src);
    let rules = program.rules();
    // 2 heads × 2 bodies = 4 rules
    assert_eq!(rules.len(), 4);
    assert_eq!(rules[0].head().name(), "c");
    assert_eq!(rules[0].rhs()[0].name(), "a");
    assert_eq!(rules[1].head().name(), "c");
    assert_eq!(rules[1].rhs()[0].name(), "b");
    assert_eq!(rules[2].head().name(), "d");
    assert_eq!(rules[2].rhs()[0].name(), "a");
    assert_eq!(rules[3].head().name(), "d");
    assert_eq!(rules[3].rhs()[0].name(), "b");
}

/// Diamond include: `root` includes `left` and `right`, both include
/// `leaf`. The `completed` set in `resolve_includes` must prevent
/// `leaf` from being inlined twice — otherwise `.decl leaf_rel` would
/// appear twice and fail with `DuplicateDecl`. This guards the warn-
/// and-skip branch at the `completed.contains` check.
#[test]
fn diamond_include_dedups_leaf() {
    let dir = tempdir().expect("tempdir");
    let write = |name: &str, body: &str| {
        fs::write(dir.path().join(name), body).expect("write");
    };
    write(
        "leaf.dl",
        ".decl leaf_rel(x: number)\n.output leaf_rel\nleaf_rel(1).\n",
    );
    write("left.dl", ".include \"leaf.dl\"\n");
    write("right.dl", ".include \"leaf.dl\"\n");
    write("root.dl", ".include \"left.dl\"\n.include \"right.dl\"\n");

    let mut sm = SourceMap::new();
    let program = Program::parse(
        &dir.path().join("root.dl").to_string_lossy(),
        true,
        &[],
        &mut sm,
    )
    .expect("diamond include should succeed with dedup");

    let rels: Vec<_> = program
        .relations()
        .iter()
        .filter(|r| r.name() == "leaf_rel")
        .collect();
    assert_eq!(rels.len(), 1, "leaf_rel inlined twice");
}

/// Chained aliases: `A = B = C = number`. All four resolve to int32.
#[test]
fn type_alias_chain_resolves_to_root() {
    let src = "
        .type C = number
        .type B = C
        .type A = B
        .decl R(x: A, y: B, z: C)
        .output R
        R(1, 2, 3).
    ";
    let program = parse_program(src);
    let r = find_relation(&program, "r");
    assert_eq!(
        r.data_type(),
        vec![DataType::Int32, DataType::Int32, DataType::Int32]
    );
}

// =============================================================
// `.override` / `overridable`
// =============================================================
//
// The inliner replaces user-written `.` with `·` (U+00B7) in
// prefixed relation names, so a `.init s = Sub` produces facts
// keyed by `s·foo` rather than `s.foo`. The tests below assert
// against the post-inliner form.

/// Extract the first-column integer values from `program`'s facts
/// for `rel`. Used by the override tests, which assert by tuple
/// value rather than by parsed-AST structure.
fn fact_numbers(program: &Program, rel: &str) -> Vec<i64> {
    program
        .facts()
        .get(rel)
        .unwrap_or_else(|| panic!("no facts for `{rel}`"))
        .iter()
        .map(|(_, tuple)| match &tuple[0] {
            ConstType::Int(n) => *n,
            other => panic!("expected number in `{rel}`, got {other:?}"),
        })
        .collect()
}

/// `.output R` and `.printsize R` on the same relation conflict —
/// both target `R.csv`, the second would silently clobber the
/// first. Reject at parse time.
#[test]
fn output_and_printsize_on_same_relation_rejected() {
    let err = parse_program_result(
        "
        .decl R(x: number)
        R(1).
        .output R
        .printsize R
        ",
    )
    .unwrap_err();
    assert!(
        matches!(&err, ParseError::OutputAndPrintsizeConflict { name, .. } if name == "R"),
        "expected OutputAndPrintsizeConflict, got {err:?}"
    );
}

/// Order-independent: `.printsize R` before `.output R` is the
/// same conflict and must also be rejected.
#[test]
fn output_and_printsize_rejected_regardless_of_order() {
    let err = parse_program_result(
        "
        .decl R(x: number)
        R(1).
        .printsize R
        .output R
        ",
    )
    .unwrap_err();
    assert!(
        matches!(err, ParseError::OutputAndPrintsizeConflict { .. }),
        "expected OutputAndPrintsizeConflict, got {err:?}"
    );
}

/// Comp-internal directives bypass `apply_directives` (the
/// inliner sets the flags directly), so the conflict check must
/// run AFTER both passes. This pins that — without the post-pass
/// validator the conflict would slip through and produce two
/// writers racing on the same `c.R.csv` file.
#[test]
fn output_and_printsize_inside_comp_rejected() {
    let err = parse_program_result(
        "
        .comp C {
          .decl Src(x: number)
          .decl R(x: number)
          Src(1).
          R(x) :- Src(x).
          .output R
          .printsize R
        }
        .init c = C
        ",
    )
    .unwrap_err();
    assert!(
        matches!(err, ParseError::OutputAndPrintsizeConflict { .. }),
        "expected OutputAndPrintsizeConflict for comp-internal pair, got {err:?}"
    );
}

/// `.output R` with no rules, no facts, and no body references is pruned from
/// the dataflow (so codegen doesn't emit a buffer for a non-existent node). A
/// `warn!` from the pruning pass surfaces it to the user.
#[test]
fn empty_output_pruned_from_dataflow() {
    let program = parse_program(
        "
        .decl Nothing(x: symbol)
        .decl Src(x: symbol)
        .decl Out(x: symbol)
        Src(\"v\").
        Out(x) :- Src(x).
        .output Nothing
        .output Out
        ",
    );
    // `Nothing` is pruned from output_idbs (no rules, no facts, unreferenced).
    assert!(
        program.output_idbs().iter().all(|r| r.name() != "nothing"),
        "empty `.output` should be pruned from output_idbs, got: {:?}",
        program
            .output_idbs()
            .iter()
            .map(|r| r.name())
            .collect::<Vec<_>>()
    );
    // `Out` flows through the normal drain path.
    assert!(program.output_idbs().iter().any(|r| r.name() == "out"));
}

/// Infix `cat` is gone — `a cat b` MUST NOT parse as a string
/// concatenation. A future re-introduction would silently change
/// the grammar; this test pins that.
#[test]
fn infix_cat_no_longer_parses() {
    let res = parse_program_result(
        "
        .decl A(x: symbol)
        .decl B(x: symbol)
        .decl C(x: symbol)
        A(\"hi\").
        B(\"there\").
        C(x cat y) :- A(x), B(y).
        ",
    );
    assert!(
        res.is_err(),
        "infix `cat` should be a parse error, but parsed: {res:?}"
    );
}

/// Inliner normalizes dotted instance names (`c.R` → `c·R`) on
/// `name` for Rust ident safety, but leaves `raw_name` carrying
/// the original literal-dot form — that's what the I/O sinks use
/// for Soufflé-style filenames (`c.R.csv`, not `c·R.csv`).
#[test]
fn inlined_relation_raw_name_keeps_literal_dot() {
    let src = "
        .comp C {
          .decl R(x: symbol)
          .decl S(x: symbol)
          R(x) :- S(x).
          .output R
        }
        .init c = C
    ";
    let program = parse_program(src);
    let r = find_relation(&program, "c·r");
    assert_eq!(r.name(), "c·r");
    assert_eq!(r.raw_name(), "c.R");
}

// =============================================================
// Qualified member types of nested / type-param-bound `.init`s
// =============================================================

/// Facet A: an attribute typed `instance.Member` resolves even when
/// the nested `.init` that supplies `Member` is declared *after* the
/// `.decl` in the comp body — attribute-type resolution is independent
/// of textual order within the comp body (Soufflé-compatible).
#[test]
fn member_type_resolves_when_nested_init_follows_decl() {
    let src = "
        .type Value = symbol
        .comp Cfg { .type Context = symbol }
        .comp Analysis<Configuration> {
          .decl RunningThread(ctx:configuration.Context, v:Value)
          .init configuration = Configuration
        }
        .init mainAnalysis = Analysis<Cfg>
    ";
    let program = parse_program(src);
    let r = find_relation(&program, "mainanalysis·runningthread");
    assert_eq!(r.data_type(), vec![DataType::String, DataType::String]);
}

/// Facet B: a base component declares relations over
/// `configuration.Member` where `configuration` is the eventual
/// instance of that component itself (no local `.init`), and the
/// member `.type` is supplied by a concrete subtype. When the
/// outermost `.init` binds the subtype, `configuration.Member`
/// resolves to the subtype's `.type`.
#[test]
fn self_referential_member_type_from_concrete_subtype() {
    let src = "
        .type Value = symbol
        .type Invo = symbol
        .comp AbstractConfiguration {
          .decl ContextRequest(ctx:configuration.Context, invo:Invo)
        }
        .comp Analysis<Configuration> {
          .init configuration = Configuration
          .decl RunningThread(ctx:configuration.Context, v:Value)
        }
        .comp ConcreteConfiguration : AbstractConfiguration {
          .type Context = symbol
        }
        .init mainAnalysis = Analysis<ConcreteConfiguration>
    ";
    let program = parse_program(src);
    let req = find_relation(&program, "mainanalysis·configuration·contextrequest");
    assert_eq!(req.data_type(), vec![DataType::String, DataType::String]);
    let thread = find_relation(&program, "mainanalysis·runningthread");
    assert_eq!(thread.data_type(), vec![DataType::String, DataType::String]);
}

/// A component-local `.type` alias used as a *bare* (unqualified)
/// attribute type within the same component must resolve, matching
/// top-level `.type` alias behaviour. The alias is registered under
/// the instance prefix, so a bare use-site must resolve against the
/// instance-local alias table.
#[test]
fn comp_local_type_alias_resolves_as_attr_type() {
    let src = "
        .comp C {
          .type MethodType = symbol
          .decl R(mt:MethodType, i:number)
        }
        .init c = C
    ";
    let program = parse_program(src);
    let r = find_relation(&program, "c·r");
    assert_eq!(r.data_type(), vec![DataType::String, DataType::Int32]);
}

/// A *bare* member type declared by a concrete subtype resolves in an
/// inherited base-component `.decl`. Inheritance flattens the base body
/// and the subtype's `.type` into one comp body, so the subtype's alias
/// is local — the same mechanism as a plain component-local alias.
#[test]
fn bare_member_type_from_concrete_subtype_resolves() {
    let src = "
        .type Invo = symbol
        .comp AbstractConfiguration {
          .decl ContextRequest(ctx:Context, invo:Invo)
        }
        .comp ConcreteConfiguration : AbstractConfiguration {
          .type Context = symbol
        }
        .init c = ConcreteConfiguration
    ";
    let program = parse_program(src);
    let r = find_relation(&program, "c·contextrequest");
    assert_eq!(r.data_type(), vec![DataType::String, DataType::String]);
}

/// A rule inside one component may reference a relation of a *sibling*
/// instance declared in the enclosing (global) scope. `basic` and
/// `main` are both global `.init`s; `basic.SubtypeOf` inside `main`'s
/// body resolves to the global `basic·subtypeof` relation (Soufflé
/// sibling/enclosing-scope visibility).
#[test]
fn sibling_instance_relation_ref_resolves() {
    let src = "
        .comp Lib { .decl SubtypeOf(a:symbol, b:symbol) }
        .init basic = Lib
        .comp Analysis {
          .decl R(x:symbol)
          R(x) :- basic.SubtypeOf(x, _).
        }
        .init main = Analysis
    ";
    let program = parse_program(src);
    let rule = program
        .rules()
        .into_iter()
        .find(|r| r.head().name() == "main·r")
        .expect("main·r rule");
    let body: Vec<&str> = rule.rhs().iter().map(|p| p.name()).collect();
    assert!(
        body.contains(&"basic·subtypeof"),
        "sibling ref should resolve to basic·subtypeof, got {body:?}"
    );
}

/// `R(t) :- A(x), t = x + 1.` — the assignment grounds `t`, which is
/// substituted into the head as an arithmetic expression; the equality
/// literal is dropped, leaving only the positive atom.
#[test]
fn equality_assignment_arith_substituted_into_head() {
    let program = parse_program(
        "
        .decl A(x:number)
        .decl R(t:number)
        .input A(IO=\"file\",filename=\"A.csv\")
        R(t) :- A(x), t = x + 1.
        .output R
        ",
    );
    let rule = program
        .rules()
        .into_iter()
        .find(|r| r.head().name() == "r")
        .expect("r rule");
    assert_eq!(rule.rhs().len(), 1, "equality literal should be dropped");
    assert!(matches!(rule.rhs()[0], Predicate::PositiveAtom(_)));
    assert!(
        matches!(rule.head().head_arguments()[0], HeadArg::Arith(_)),
        "head arg should carry the substituted arithmetic"
    );
}

/// `R(y) :- A(x), y = x.` — a pure aliasing assignment collapses the head
/// argument back to the source variable.
#[test]
fn equality_assignment_alias_substituted_into_head() {
    let program = parse_program(
        "
        .decl A(x:symbol)
        .decl R(y:symbol)
        .input A(IO=\"file\",filename=\"A.csv\")
        R(y) :- A(x), y = x.
        .output R
        ",
    );
    let rule = program
        .rules()
        .into_iter()
        .find(|r| r.head().name() == "r")
        .expect("r rule");
    assert_eq!(rule.rhs().len(), 1);
    match &rule.head().head_arguments()[0] {
        HeadArg::Var(v) => assert_eq!(v, "x"),
        other => panic!("expected aliased Var(x), got {other:?}"),
    }
}

/// `P(t) :- t = "boolean".` — an empty-body rule whose head is fully ground
/// after substitution is lowered to an inline fact.
#[test]
fn equality_assignment_const_only_becomes_fact() {
    let program = parse_program(
        "
        .decl P(t:symbol)
        P(t) :- t = \"boolean\".
        .output P
        ",
    );
    assert!(
        program.facts().contains_key("p"),
        "const-only assignment rule should become a fact"
    );
    assert_eq!(program.facts()["p"].len(), 1);
    assert!(
        program.rules().iter().all(|r| r.head().name() != "p"),
        "no derivation rule should remain for the fact relation"
    );
}

/// `R(x,t) :- A(x), B(t), t = x.` — `t` is already bound by `B`, so the
/// equality is a genuine filter and must be left in the body, not treated
/// as an assignment.
#[test]
fn equality_between_bound_columns_is_kept_as_filter() {
    let program = parse_program(
        "
        .decl A(x:number)
        .decl B(t:number)
        .decl R(x:number, t:number)
        .input A(IO=\"file\",filename=\"A.csv\")
        .input B(IO=\"file\",filename=\"B.csv\")
        R(x, t) :- A(x), B(t), t = x.
        .output R
        ",
    );
    let rule = program
        .rules()
        .into_iter()
        .find(|r| r.head().name() == "r")
        .expect("r rule");
    let compares = rule
        .rhs()
        .iter()
        .filter(|p| matches!(p, Predicate::Compare(_)))
        .count();
    assert_eq!(compares, 1, "filter equality must be preserved");
}

/// A relation that is declared and referenced by a live rule but never
/// produced (no rule, no `.input`, no facts) is materialized as an empty
/// inline-fact relation so codegen emits an empty collection for it.
/// An `.input`-backed relation is NOT an orphan — its collection comes
/// from the fact file.
#[test]
fn orphan_relation_referenced_by_live_rule_is_materialized_empty() {
    let program = parse_program(
        "
        .decl O(x:symbol)
        .decl I(x:symbol)
        .input I(IO=\"file\",filename=\"I.csv\")
        .decl R(x:symbol)
        R(x) :- O(x), I(x).
        .output R
        ",
    );
    assert!(
        program.facts().contains_key("o"),
        "orphan relation should be materialized"
    );
    assert!(
        program.facts()["o"].is_empty(),
        "materialized orphan must be empty"
    );
    assert!(
        !program.facts().contains_key("i"),
        ".input relation must not be materialized as an orphan"
    );
}

/// Substituting an assignment of a *computed* expression into a negated
/// atom is rejected: a negated atom argument can only be a bare variable or
/// constant, never an arithmetic expression.
#[test]
fn equality_assignment_into_negation_with_arith_errors() {
    let err = parse_program_result(
        "
        .decl A(x:number)
        .decl B(t:number)
        .decl R(x:number)
        .input A(IO=\"file\",filename=\"A.csv\")
        .input B(IO=\"file\",filename=\"B.csv\")
        R(x) :- A(x), !B(t), t = x + 1.
        .output R
        ",
    )
    .expect_err("computed value into negated atom should error");
    assert!(
        matches!(err, ParseError::AssignmentVarInNegation { .. }),
        "expected AssignmentVarInNegation, got {err:?}"
    );
}

/// A rule whose body desugars away entirely must still leave the segment:
/// ground integer arithmetic in the head is folded (`x = 1 + 2` → fact
/// `P(3)`), and anything unfoldable is rejected with
/// [`ParseError::GroundRuleNotConst`] instead of reaching the planner,
/// which panics on zero-positive-atom rules.
#[test]
fn assignment_only_rule_folds_or_rejects() {
    // Foldable integer expression → inline fact P(3).
    let program = parse_program(
        "
        .decl P(x:number)
        P(x) :- x = 1 + 2.
        .output P
        ",
    );
    assert!(program.facts().contains_key("p"));
    assert!(program.rules().iter().all(|r| r.head().name() != "p"));

    // Unfoldable (builtin call) → rejected, not handed to the planner.
    let err = parse_program_result(
        "
        .decl P(s:symbol)
        P(s) :- s = cat(\"a\", \"b\").
        .output P
        ",
    )
    .expect_err("builtin in ground head should be rejected");
    assert!(
        matches!(err, ParseError::GroundRuleNotConst { .. }),
        "expected GroundRuleNotConst, got {err:?}"
    );

    // Unbound head variable with emptied body → same rejection.
    let err = parse_program_result(
        "
        .decl P(x:number)
        P(x) :- y = 1.
        .output P
        ",
    )
    .expect_err("unbound head var in ground rule should be rejected");
    assert!(
        matches!(err, ParseError::GroundRuleNotConst { .. }),
        "expected GroundRuleNotConst, got {err:?}"
    );

    // Division by zero refuses to fold → rejected, not miscomputed.
    let err = parse_program_result(
        "
        .decl P(x:number)
        P(x) :- x = 1 / 0.
        .output P
        ",
    )
    .expect_err("division by zero should be rejected");
    assert!(matches!(err, ParseError::GroundRuleNotConst { .. }));

    // Folding recurses through groups: (1 + 2) * 3 → fact P(9).
    let program = parse_program(
        "
        .decl P(x:number)
        P(x) :- x = (1 + 2) * 3.
        .output P
        ",
    );
    assert!(program.facts().contains_key("p"));
}

/// Chained assignments resolve to a fixpoint regardless of source order
/// (`b = a + 2` is discovered only after `a = x + 1` grounds `a`), and the
/// resolved values substitute into remaining comparison filters.
#[test]
fn chained_assignments_resolve_and_substitute_into_filters() {
    let program = parse_program(
        "
        .decl A(x:number)
        .decl R(a:number, b:number)
        .input A(IO=\"file\",filename=\"A.csv\")
        R(a, b) :- A(x), b = a + 2, a = x + 1, b < 10.
        .output R
        ",
    );
    let rule = program
        .rules()
        .into_iter()
        .find(|r| r.head().name() == "r")
        .expect("r rule");
    // Both assignments eliminated; only the `< 10` filter survives, and it
    // no longer mentions the assignment variables.
    let mut compare_vars = Vec::new();
    let mut compares = 0;
    for pred in rule.rhs() {
        if let Predicate::Compare(e) = pred {
            compares += 1;
            compare_vars.extend(e.left().vars().into_iter().cloned());
            compare_vars.extend(e.right().vars().into_iter().cloned());
        }
    }
    assert_eq!(compares, 1, "only the filter comparison remains");
    assert!(
        compare_vars.iter().all(|v| v == "x"),
        "assignment vars must be fully substituted away: {rule}"
    );
}

/// A multi-term assignment value spliced into a factor slot is wrapped in
/// a `Group`, preserving fold order: `z = x * y` with `y := a - b` must
/// mean `x * (a - b)`, not `(x * a) - b`. Same inside aggregation args.
#[test]
fn multi_term_substitution_wraps_in_group() {
    use crate::Factor;

    let program = parse_program(
        "
        .decl A(x:number, a:number, b:number)
        .decl R(z:number)
        .input A(IO=\"file\",filename=\"A.csv\")
        R(z) :- A(x, a, b), y = a - b, z = x * y.
        .output R
        ",
    );
    let rule = program
        .rules()
        .into_iter()
        .find(|r| r.head().name() == "r")
        .expect("r rule");
    let [HeadArg::Arith(arith)] = rule.head().head_arguments() else {
        panic!("expected one arithmetic head arg");
    };
    let (_, factor) = &arith.rest()[0];
    assert!(
        matches!(factor, Factor::Group(inner) if !inner.rest().is_empty()),
        "substituted multi-term value must be group-wrapped: {arith}"
    );

    // Aggregation arguments take the same substitution path.
    let program = parse_program(
        "
        .decl A(g:number, x:number)
        .decl S(g:number, s:number)
        .input A(IO=\"file\",filename=\"A.csv\")
        S(g, sum(t)) :- A(g, x), t = x + 1.
        .output S
        ",
    );
    let rule = program
        .rules()
        .into_iter()
        .find(|r| r.head().name() == "s")
        .expect("s rule");
    let agg = rule
        .head()
        .head_arguments()
        .iter()
        .find_map(|a| match a {
            HeadArg::Aggregation(agg) => Some(agg),
            _ => None,
        })
        .expect("aggregation head arg");
    assert!(
        !agg.arithmetic().vars().iter().any(|v| *v == "t"),
        "assignment var must be substituted inside the aggregation"
    );
}

/// The desugar pass also runs inside `fixpoint`/`loop` blocks.
#[test]
fn assignment_inside_fixpoint_desugared() {
    let program = parse_program(
        "
        .decl A(x:number)
        .decl R(t:number)
        .input A(IO=\"file\",filename=\"A.csv\")
        fixpoint {
          R(t) :- A(x), t = x + 1.
          R(t) :- R(x), t = x + 1, x < 5.
        }
        .output R
        ",
    );
    for rule in program.rules() {
        let assignments = rule
            .rhs()
            .iter()
            .filter(|p| matches!(p, Predicate::Compare(e) if *e.operator() == ComparisonOperator::Equal))
            .count();
        assert_eq!(assignments, 0, "assignments inside blocks must desugar");
    }
}

/// Parentheses around a single factor are transparent: a grouped constant
/// assignment `t = ("boolean")` lowers to a fact exactly like the bare
/// form, a grouped assignment variable `(t) = x` is still recognized as
/// an assignment, and a grouped bare variable substituted into a negated
/// atom is accepted (it is not computed arithmetic).
#[test]
fn single_factor_groups_are_transparent_to_desugar() {
    // Grouped constant → inline fact (not an empty-bodied rule).
    let program = parse_program(
        "
        .decl P(t:symbol)
        P(t) :- t = (\"boolean\").
        .output P
        ",
    );
    assert!(
        program.facts().contains_key("p"),
        "grouped const-only assignment rule should become a fact"
    );

    // Grouped assignment variable → recognized and eliminated.
    let program = parse_program(
        "
        .decl A(x:number)
        .decl R(t:number)
        .input A(IO=\"file\",filename=\"A.csv\")
        R(t) :- A(x), (t) = x.
        .output R
        ",
    );
    let rule = program
        .rules()
        .into_iter()
        .find(|r| r.head().name() == "r")
        .expect("r rule");
    assert!(
        !rule
            .rhs()
            .iter()
            .any(|p| matches!(p, Predicate::Compare(_))),
        "grouped assignment must be eliminated, not left as a filter"
    );

    // Grouped bare variable into a negated atom → fine, not arithmetic.
    parse_program(
        "
        .decl A(x:number)
        .decl B(t:number)
        .decl R(x:number)
        .input A(IO=\"file\",filename=\"A.csv\")
        .input B(IO=\"file\",filename=\"B.csv\")
        R(x) :- A(x), !B(t), t = (x).
        .output R
        ",
    );
}

/// An `.output`/`.input` directive *inside* a component may target a
/// relation declared in the enclosing (global) scope. The directive
/// resolver falls through to the global relation set — the same scope
/// fall-through rule-body references already use.
#[test]
fn comp_directive_targets_global_relation() {
    let src = "
        .decl G(x:symbol)
        G(\"a\").
        .comp C {
          .decl L(x:symbol)
          L(x) :- G(x).
          .output G(IO=\"file\",filename=\"G.csv\",delimiter=\"\\t\")
        }
        .init c = C
    ";
    let program = parse_program(src);
    let g = find_relation(&program, "g");
    assert!(
        g.output(),
        ".output of a global relation from inside a comp should apply"
    );
}

/// Spec test 1: `.output Foo` drops parent's ground facts.
#[test]
fn override_drops_parent_facts() {
    let src = "
        .comp Base {
          .decl Foo(x: number) overridable
          Foo(1).
          Foo(2).
        }
        .comp Sub : Base {
          .override Foo
          Foo(10).
        }
        .init s = Sub
        .output s.Foo
    ";
    let program = parse_program(src);
    assert_eq!(fact_numbers(&program, "s·foo"), vec![10]);
}

/// Spec test 2: override replaces a derived rule, not just facts.
#[test]
fn override_drops_parent_derived_rule() {
    let src = "
        .comp Base {
          .decl Foo(x: number) overridable
          .decl Seed(x: number)
          Foo(x) :- Seed(x).
        }
        .comp Sub : Base {
          .override Foo
          Foo(x) :- Seed(x), x > 5.
        }
        .init s = Sub
        .input s.Seed(IO=\"file\", filename=\"Seed.csv\", delimiter=\",\")
        .output s.Foo
    ";
    let program = parse_program(src);
    let rules: Vec<_> = program
        .rules()
        .into_iter()
        .filter(|r| r.head().name() == "s·foo")
        .collect();
    assert_eq!(rules.len(), 1, "exactly one s·foo rule survives");
    // The surviving rule must carry the `x > 5` filter.
    let has_compare = rules[0]
        .rhs()
        .iter()
        .any(|p| matches!(p, Predicate::Compare(_)));
    assert!(has_compare, "override's filtered rule should survive");
}

/// Spec test 3: `overridable` without `.override` is a no-op.
#[test]
fn overridable_without_override_keeps_parent_facts() {
    let src = "
        .comp Base {
          .decl Foo(x: number) overridable
          Foo(1).
        }
        .comp Sub : Base { }
        .init s = Sub
        .output s.Foo
    ";
    let program = parse_program(src);
    let tuples = program.facts().get("s·foo").expect("s·foo facts");
    assert_eq!(tuples.len(), 1);
}

/// Spec test 4: `.override` of a non-`overridable` parent is an error.
#[test]
fn override_of_non_overridable_errors() {
    let src = "
        .comp Base { .decl Foo(x: number)  Foo(1). }
        .comp Sub : Base { .override Foo  Foo(10). }
        .init s = Sub
        .output s.Foo
    ";
    let err = parse_program_result(src).unwrap_err();
    assert!(
        matches!(err, ParseError::OverrideOfNonOverridable { .. }),
        "got {err:?}"
    );
}

/// Spec test 5: `.override` of an unknown relation name is an error.
#[test]
fn override_unknown_relation_errors() {
    let src = "
        .comp Base { .decl Foo(x: number) overridable  Foo(1). }
        .comp Sub : Base { .override Bar  Foo(10). }
        .init s = Sub
        .output s.Foo
    ";
    let err = parse_program_result(src).unwrap_err();
    assert!(
        matches!(err, ParseError::OverrideUnknownRelation { .. }),
        "got {err:?}"
    );
}

/// Spec test 6: diamond inheritance — Mid's override wins through to Bot.
#[test]
fn override_propagates_through_inheritance_chain() {
    let src = "
        .comp Top  { .decl Foo(x: number) overridable  Foo(1). }
        .comp Mid1 : Top { .override Foo  Foo(2). }
        .comp Bot  : Mid1 { }
        .init b = Bot
        .output b.Foo
    ";
    let program = parse_program(src);
    assert_eq!(fact_numbers(&program, "b·foo"), vec![2]);
}

/// Spec test 7: parametric — `.comp X<T>` declares `Foo(x: T) overridable`,
/// subcomponent overrides it; type substitution must still flow through.
#[test]
fn override_parametric_type_substitution() {
    let src = "
        .comp Base<T> {
          .decl Foo(x: T) overridable
          Foo(0).
        }
        .comp Sub<T> : Base<T> {
          .override Foo
          Foo(42).
        }
        .init s = Sub<number>
        .output s.Foo
    ";
    let program = parse_program(src);
    assert_eq!(fact_numbers(&program, "s·foo"), vec![42]);
}

/// Spec test 8: `.override` with zero replacement rules drops the
/// parent's derivations and leaves nothing in their place. (FlowLog
/// then prunes the empty relation downstream — Soufflé would keep
/// it; either way the override successfully cleared the parent's
/// rules, which is what this test guards.)
#[test]
fn override_to_empty_drops_parent_derivations() {
    let src = "
        .comp Base {
          .decl Foo(x: number) overridable
          Foo(1).
        }
        .comp Sub : Base {
          .override Foo
        }
        .init s = Sub
        .output s.Foo
    ";
    let program = parse_program(src);
    assert!(program.facts().get("s·foo").is_none());
    assert!(program.rules().iter().all(|r| r.head().name() != "s·foo"));
}

/// Spec test 9: `.override` at the top level is a syntax error.
#[test]
fn override_outside_comp_is_syntax_error() {
    let src = "
        .decl Foo(x: number)
        .override Foo
        .output Foo
    ";
    let err = parse_program_result(src).unwrap_err();
    assert!(matches!(err, ParseError::Syntax { .. }), "got {err:?}");
}

/// Spec test 10: subcomponent that both `.override`s AND re-`.decl`s
/// the relation must fail with `OverrideRedeclaresRelation`.
#[test]
fn override_redeclaration_errors() {
    let src = "
        .comp Base {
          .decl Foo(x: number) overridable
          Foo(1).
        }
        .comp Sub : Base {
          .override Foo
          .decl Foo(x: number)
          Foo(10).
        }
        .init s = Sub
        .output s.Foo
    ";
    let err = parse_program_result(src).unwrap_err();
    assert!(
        matches!(err, ParseError::OverrideRedeclaresRelation { .. }),
        "got {err:?}"
    );
}

/// Spec test 11: two `.override Foo` directives are redundant but
/// not an error — they collapse to a single override.
#[test]
fn double_override_is_accepted() {
    let src = "
        .comp Base {
          .decl Foo(x: number) overridable
          Foo(1).
        }
        .comp Sub : Base {
          .override Foo
          .override Foo
          Foo(10).
        }
        .init s = Sub
        .output s.Foo
    ";
    let program = parse_program(src);
    let tuples = program.facts().get("s·foo").expect("s·foo facts");
    assert_eq!(tuples.len(), 1);
}

/// `overridable` outside a `.comp` body must be rejected with a
/// dedicated error rather than silently accepted.
#[test]
fn overridable_outside_comp_errors() {
    let src = ".decl Foo(x: number) overridable\n.output Foo\n";
    let err = parse_program_result(src).unwrap_err();
    assert!(
        matches!(err, ParseError::OverridableOutsideComp { .. }),
        "got {err:?}"
    );
}

// =============================================================
// `.plan` directive
// =============================================================

/// Source-level body order is permuted in the parsed AST so that
/// downstream (planner, optimizer) sees the hinted order as if the
/// user had written the body that way directly.
#[test]
fn plan_reorders_positive_atoms() {
    let src = "
        .decl a(x: number)
        .decl b(x: number)
        .decl c(x: number)
        .decl h(x: number)
        .output h
        h(X) :- a(X), b(X), c(X).
        .plan (3, 1, 2)
    ";
    let program = parse_program(src);
    let rule = program.rules()[0];
    let names: Vec<&str> = rule.rhs().iter().map(|p| p.name()).collect();
    assert_eq!(names, vec!["c", "a", "b"]);
}

/// Soufflé's `.plan N:(…)` form is accepted as an alias: the
/// leading version index is stripped and the permutation is
/// applied just like the native form.
#[test]
fn plan_souffle_form_applies_permutation() {
    let src = "
        .decl a(x: number)
        .decl b(x: number)
        .decl c(x: number)
        .decl h(x: number)
        .output h
        h(X) :- a(X), b(X), c(X).
        .plan 1:(3, 1, 2)
    ";
    let program = parse_program(src);
    let rule = program.rules()[0];
    let names: Vec<&str> = rule.rhs().iter().map(|p| p.name()).collect();
    assert_eq!(names, vec!["c", "a", "b"]);
}

/// `.plan` indices are positive-atom-only. Negations stay in their
/// original global slots; the positive-atom permutation slides under
/// them.
#[test]
fn plan_skips_negations_and_pins_only_positives() {
    let src = "
        .decl a(x: number)
        .decl b(x: number)
        .decl c(x: number)
        .decl d(x: number)
        .decl h(x: number)
        .output h
        h(X) :- a(X), !d(X), b(X), c(X).
        .plan (3, 1, 2)
    ";
    let program = parse_program(src);
    let rule = program.rules()[0];
    let labelled: Vec<String> = rule
        .rhs()
        .iter()
        .map(|p| match p {
            Predicate::PositiveAtom(a) => a.name().to_string(),
            Predicate::NegativeAtom(a) => format!("!{}", a.name()),
            other => format!("{other}"),
        })
        .collect();
    // Positive slots [a, b, c] permute to [c, a, b]; `!d` keeps slot 1.
    assert_eq!(labelled, vec!["c", "!d", "a", "b"]);
}

/// `.plan` with no preceding rule clause is rejected, not silently
/// dropped or attached to something earlier.
#[test]
fn plan_without_preceding_rule_errors() {
    let src = "
        .decl a(x: number)
        .output a
        .plan (1)
    ";
    let err = parse_program_result(src).unwrap_err();
    assert!(matches!(err, ParseError::PlanOrphan { .. }), "got {err:?}");
}

/// Index count must equal the positive-atom count.
#[test]
fn plan_arity_mismatch_errors() {
    let src = "
        .decl a(x: number)
        .decl b(x: number)
        .decl h(x: number)
        .output h
        h(X) :- a(X), b(X).
        .plan (1, 2, 3)
    ";
    let err = parse_program_result(src).unwrap_err();
    assert!(
        matches!(
            err,
            ParseError::PlanArityMismatch {
                expected: 2,
                found: 3,
                ..
            }
        ),
        "got {err:?}"
    );
}

/// Index 0 or > positive-atom-count is rejected.
#[test]
fn plan_index_out_of_range_errors() {
    let src = "
        .decl a(x: number)
        .decl b(x: number)
        .decl h(x: number)
        .output h
        h(X) :- a(X), b(X).
        .plan (1, 3)
    ";
    let err = parse_program_result(src).unwrap_err();
    assert!(
        matches!(
            err,
            ParseError::PlanIndexOutOfRange {
                index: 3,
                max: 2,
                ..
            }
        ),
        "got {err:?}"
    );
}

/// Each index must appear exactly once.
#[test]
fn plan_duplicate_index_errors() {
    let src = "
        .decl a(x: number)
        .decl b(x: number)
        .decl h(x: number)
        .output h
        h(X) :- a(X), b(X).
        .plan (1, 1)
    ";
    let err = parse_program_result(src).unwrap_err();
    assert!(
        matches!(err, ParseError::PlanDuplicateIndex { index: 1, .. }),
        "got {err:?}"
    );
}

/// `.plan` is honored inside `fixpoint { ... }` blocks.
#[test]
fn plan_inside_fixpoint_block() {
    let src = "
        .decl a(x: number)
        .decl b(x: number)
        .decl c(x: number)
        .decl h(x: number)
        .output h
        fixpoint {
            h(X) :- a(X), b(X), c(X).
            .plan (3, 1, 2)
        }
    ";
    let program = parse_program(src);
    let block = loop_blocks(&program)[0];
    let names: Vec<&str> = block.rules()[0].rhs().iter().map(|p| p.name()).collect();
    assert_eq!(names, vec!["c", "a", "b"]);
}

/// `.plan` inside a `.comp` body permutes the rule's positive atoms
/// at parse time, so the inlined / prefixed rule reaches the planner
/// already in hint order.
#[test]
fn plan_inside_comp_body() {
    let src = "
        .comp C {
          .decl A(x: number)
          .decl B(x: number)
          .decl D(x: number)
          .decl H(x: number)
          H(X) :- A(X), B(X), D(X).
          .plan (3, 1, 2)
        }
        .init c = C
        .output c.H
    ";
    let program = parse_program(src);
    let rule = program
        .rules()
        .into_iter()
        .find(|r| r.head().name() == "c·h")
        .expect("instantiated H rule");
    let names: Vec<&str> = rule.rhs().iter().map(|p| p.name()).collect();
    assert_eq!(names, vec!["c·d", "c·a", "c·b"]);
    assert!(rule.plan_pinned(), "plan_pinned should survive inlining");
}

/// `.plan` orphan inside a `.comp` is still rejected.
#[test]
fn plan_orphan_inside_comp_errors() {
    let src = "
        .comp C {
          .decl A(x: number)
          .plan (1)
        }
        .init c = C
        .output c.A
    ";
    let err = parse_program_result(src).unwrap_err();
    assert!(matches!(err, ParseError::PlanOrphan { .. }), "got {err:?}");
}
