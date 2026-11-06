//! Exports planned column references without reading benchmark facts.

use std::collections::BTreeMap;
use std::error::Error;

use flowlog_common::Config;
use flowlog_common::SourceMap;
use flowlog_planner::planner::ArithmeticArgument;
use flowlog_planner::planner::Collection;
use flowlog_planner::planner::FactorArgument;
use flowlog_planner::planner::ProgramPlanner;
use flowlog_planner::planner::TransformationArgument;
use serde_json::Value;
use serde_json::json;

fn collection(value: &Collection) -> Value {
    json!({
        "id": format!("{:016x}", value.fingerprint()),
        "arity": value.arity(),
        "label": value.to_string(),
    })
}

fn expression(value: &ArithmeticArgument) -> Value {
    let slot = if let (FactorArgument::Var(slot), true) = (value.init(), value.rest().is_empty()) {
        match slot {
            TransformationArgument::KV((key, index)) => Some(json!([0, key, index])),
            TransformationArgument::Jn((left, key, index)) => {
                Some(json!([if *left { 0 } else { 1 }, key, index]))
            }
        }
    } else {
        None
    };
    json!({"slot": slot, "expression": value.to_string(),
           "constant": matches!(value.init(), FactorArgument::Const(_))
                        && value.rest().is_empty()})
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let path = std::env::args().nth(1).ok_or("expected a program path")?;
    let mut source_map = SourceMap::new();
    let mut config = Config::default();
    let program = flowlog_parser::parse_file(&path, &[], &mut source_map, &mut config)?;
    let plan = ProgramPlanner::from_program(&program, &mut None)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let strata: Vec<_> =
        plan.strata()
            .iter()
            .enumerate()
            .map(|(index, stratum)| {
                let transformations: Vec<_> =
                    [
                        (false, stratum.non_recursive_transformations()),
                        (true, stratum.recursive_transformations()),
                    ]
                    .into_iter()
                    .flat_map(|(recursive, transformations)| {
                        transformations.iter().map(move |tx| {
                let inputs = if tx.is_unary() {
                    vec![collection(tx.unary_input())]
                } else {
                    vec![collection(tx.binary_input().0), collection(tx.binary_input().1)]
                };
                json!({
                    "kind": tx.profile_operation_name(),
                    "arranged": tx.need_arrange(),
                    "recursive": recursive,
                    "inputs": inputs,
                    "output": collection(tx.output()),
                    "key": tx.flow().key().iter().map(expression).collect::<Vec<_>>(),
                    "value": tx.flow().value().iter().map(expression).collect::<Vec<_>>(),
                })
            })
                    })
                    .collect();
                let heads: BTreeMap<_, _> = stratum
                    .idb_to_heads_map()
                    .iter()
                    .map(|(relation, heads)| {
                        (
                            format!("{relation:016x}"),
                            heads
                                .iter()
                                .map(|head| format!("{head:016x}"))
                                .collect::<Vec<_>>(),
                        )
                    })
                    .collect();
                let aggregates: Vec<_> = stratum
                    .non_recursive_transformations()
                    .iter()
                    .chain(stratum.recursive_transformations())
                    .filter(|tx| {
                        matches!(
                            tx.flow(),
                            flowlog_planner::planner::TransformationFlow::Aggregate { .. }
                        )
                    })
                    .map(|tx| format!("{:016x}", tx.output().fingerprint()))
                    .collect();
                json!({"index": index, "recursive": stratum.is_recursive(),
               "transformations": transformations, "heads": heads,
               "aggregates": aggregates})
            })
            .collect();
    let relations: Vec<_> = program
        .relations()
        .iter()
        .map(|relation| {
            json!({
                "id": format!("{:016x}", relation.fingerprint()),
                "name": relation.raw_name(),
                "attributes": relation.attributes().iter().map(|a| a.name()).collect::<Vec<_>>(),
                "lattice": program.is_lattice(relation.fingerprint()),
                "edb": program.edb_fingerprints().contains(&relation.fingerprint()),
            })
        })
        .collect();
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "program": path, "rules": program.rules().len(),
            "relations": relations, "strata": strata,
        }))?
    );
    Ok(())
}
