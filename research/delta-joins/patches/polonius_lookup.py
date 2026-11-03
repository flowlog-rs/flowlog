"""Polonius (batch): lookup chains for the two rules that arrange path_maybe_uninitialized_on_exit (pmuoe).

RULES (comma list):
  me: move_error(P,T) :- pmuoe(P,S), cfg_edge(S,T).   Non-recursive rule in a later stratum: the binary
      plan arranges all of pmuoe (~291M rows on borrow) by S, only to probe cfg_edge. The lookup streams
      pmuoe rows through cfg_edge-by-c0 instead.
  pm: pmuoe(P,p2) :- pmuoe(P,p1), cfg_edge(p1,p2), !path_assigned_at(P,p2).   One recursive atom: the
      binary plan arranges every dpmuoe by p1 to join with static cfg_edge. The lookup replaces that first
      hop; the antijoin keeps its arrangement (the runtime has no anti-lookup).
  sa: subset(o1,o3,p) :- subset(o1,o2,p), subset_base(o2,o3,p), o1 != o3.   One recursive atom: the lookup
      streams dsubset through subset_base by (o2,p) instead of arranging it by (o2,p).
  sb: subset(o1,o2,p2) :- subset(o1,o2,p1), cfg_edge(p1,p2), origin_live_on_entry(o1,p2),
      origin_live_on_entry(o2,p2).   One recursive atom of four: the binary plan arranges dsubset by p1 and
      both intermediates, each only to join a static atom. One lookup chain replaces all three.
  The subset rules share a scope, so their rewrites name the runtime's operators in full rather than
  adding `use` lines.
"""

import os
import sys
from pathlib import Path

from _edit import replace_span

rules = {rule.strip() for rule in os.environ.get("RULES", "me").split(",") if rule.strip()}
assert rules and rules <= {"me", "pm", "sa", "sb"}, rules
OPS = "::flowlog_runtime::operators::"
USE = "use ::flowlog_runtime::operators::{flowlog_path, flowlog_lookup, flowlog_land, Version};"

main_rs = Path(sys.argv[1])
code = main_rs.read_text()
if "pm" in rules:
    start = '''let t_1717945629491823714 = ::flowlog_runtime::operators::flowlog_map(
                                            recursive_rel_33_path_maybe_uninitialized_on_exit.clone(),'''
    new = USE + '''
                                        // LOOKUP EXPERIMENT (pmuoe rule, first hop): dpmuoe -> cfg_edge by p1, no dpmuoe arrangement.
                                        let t_2272980431566991997 = flowlog_land(
                                            flowlog_lookup(
                                                flowlog_path(recursive_rel_33_path_maybe_uninitialized_on_exit.clone(), "Path: dpmuoe"),
                                                in_t_15642038425350907409_arr.clone(),
                                                "Lookup: dpmuoe * cfg_edge",
                                                Version::New,
                                                |&(_path, p1): &(i32, i32)| (p1,),
                                                |&(path, p1): &(i32, i32), rv: &(i32,)| Some(((path, rv.0), (p1,))),
                                            ),
                                            "Land: pmuoe rule",
                                        );
                                        '''
    code = replace_span(code, start, "let t_2272980431566991997_arr = ", new)

if "me" in rules:
    start = '''let t_1717945629491823714 = ::flowlog_runtime::operators::flowlog_map(
                                rel_33_path_maybe_uninitialized_on_exit.clone(),'''
    new = USE + '''
                            // LOOKUP EXPERIMENT (move_error): stream pmuoe through cfg_edge by S, no pmuoe arrangement.
                            let t_13334033041876428547 = flowlog_land(
                                flowlog_lookup(
                                    flowlog_path(rel_33_path_maybe_uninitialized_on_exit.clone(), "Path: pmuoe"),
                                    t_15642038425350907409_arr.clone(),
                                    "Lookup: pmuoe * cfg_edge",
                                    Version::New,
                                    |&(_path, src): &(i32, i32)| (src,),
                                    |&(path, _src): &(i32, i32), rv: &(i32,)| Some((path, rv.0)),
                                ),
                                "Land: move_error",
                            );
                            '''
    code = replace_span(code, start, "let rel_24_var_maybe_partly_initialized_on_exit = ", new)

if "sa" in rules:
    start = "let t_7842425581725764349 = ::flowlog_runtime::operators::flowlog_map("
    new = f'''// LOOKUP EXPERIMENT (subset rule A): dsubset -> subset_base by (o2,p), no dsubset arrangement.
                                        let t_2804588252094493415 = {OPS}flowlog_land(
                                            {OPS}flowlog_lookup(
                                                {OPS}flowlog_path(recursive_rel_17_subset.clone(), "Path: dsubset (A)"),
                                                in_t_245118626106735281_arr.clone(),
                                                "Lookup: dsubset * subset_base",
                                                {OPS}Version::New,
                                                |&(_o1, o2, p): &(i32, i32, i32)| (o2, p),
                                                |&(o1, _o2, p): &(i32, i32, i32), rv: &(i32,)| {{
                                                    if o1 != rv.0 {{ Some((o1, rv.0, p)) }} else {{ None }}
                                                }},
                                            ),
                                            "Land: subset rule A",
                                        );
                                        '''
    code = replace_span(code, start, "let t_12860165862765713844 = ::flowlog_runtime::operators::flowlog_map(", new)

if "sb" in rules:
    start = "let t_12860165862765713844 = ::flowlog_runtime::operators::flowlog_map("
    new = f'''// LOOKUP-CHAIN EXPERIMENT (subset rule B): dsubset -> cfg_edge by p1 -> exists
                                        // origin_live_on_entry(o1,p2) -> exists origin_live_on_entry(o2,p2). No dsubset
                                        // arrangement and no intermediate arrangements.
                                        let t_15583237635909280862 = {OPS}flowlog_land(
                                            {OPS}flowlog_lookup_exists(
                                                {OPS}flowlog_lookup_exists(
                                                    {OPS}flowlog_lookup(
                                                        {OPS}flowlog_path(recursive_rel_17_subset.clone(), "Path: dsubset (B)"),
                                                        in_t_15642038425350907409_arr.clone(),
                                                        "Lookup: dsubset * cfg_edge",
                                                        {OPS}Version::New,
                                                        |&(_o1, _o2, p1): &(i32, i32, i32)| (p1,),
                                                        |&(o1, o2, _p1): &(i32, i32, i32), rv: &(i32,)| Some((o1, o2, rv.0)),
                                                    ),
                                                    in_t_12649153210455874920_arr.clone(),
                                                    "Exists: origin_live_on_entry(o1,p2)",
                                                    {OPS}Version::New,
                                                    |&(o1, _o2, p2): &(i32, i32, i32)| (o1, p2),
                                                ),
                                                in_t_12649153210455874920_arr.clone(),
                                                "Exists: origin_live_on_entry(o2,p2)",
                                                {OPS}Version::New,
                                                |&(_o1, o2, p2): &(i32, i32, i32)| (o2, p2),
                                            ),
                                            "Land: subset rule B",
                                        );
                                        '''
    code = replace_span(code, start, "let next_4517156643138083525 = ", new)
main_rs.write_text(code)
