use std::fmt;

use flowlog_common::SECTION_BAR;
use flowlog_common::SUBSECTION_BAR;

use super::Program;
use crate::segment::Segment;

// =============================================================================
// Display
// =============================================================================

impl fmt::Display for Program {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{SECTION_BAR}")?;
        writeln!(f, "FlowLog DATALOG PROGRAM")?;
        writeln!(f, "{SECTION_BAR}")?;
        writeln!(f)?;

        if !self.relations.is_empty() {
            writeln!(f, "Relations")?;
            writeln!(f, "{SUBSECTION_BAR}")?;
            for rel in &self.relations {
                writeln!(f, "{}", rel)?;
            }
            writeln!(f)?;
        }

        if !self.udfs.is_empty() {
            writeln!(f, "Extern Functions")?;
            writeln!(f, "{SUBSECTION_BAR}")?;
            for udf in &self.udfs {
                writeln!(f, "{}", udf)?;
            }
            writeln!(f)?;
        }

        if !self.segments.is_empty() {
            writeln!(f, "Program (source order)")?;
            writeln!(f, "{SUBSECTION_BAR}")?;
            for (i, item) in self.segments.iter().enumerate() {
                match item {
                    Segment::Plain(rules) => {
                        writeln!(f, "[Segment {}]", i)?;
                        for rule in rules {
                            writeln!(f, "  {}", rule)?;
                        }
                    }
                    Segment::Loop(block) | Segment::Fixpoint(block) => {
                        writeln!(f, "[Loop {}]", i)?;
                        writeln!(f, "  {}", block)?;
                    }
                }
            }
            writeln!(f)?;
        }

        if !self.facts.is_empty() {
            writeln!(f, "Facts")?;
            writeln!(f, "{SUBSECTION_BAR}")?;
            for (rel_name, facts) in &self.facts {
                for (_, vals) in facts {
                    let values = vals
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    writeln!(f, "{}({}).", rel_name, values)?;
                }
            }
        }

        Ok(())
    }
}
