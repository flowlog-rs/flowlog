//! Report rendering (HTML).

use serde::Serialize;
use serde_json::to_string;

use crate::Result;
use crate::view::Report;

const TEMPLATE: &str = include_str!("../templates/report.html");

#[derive(Serialize)]
struct ReportWrapper<'a> {
    snapshot_labels: Vec<&'a str>,
    snapshots: &'a [Report],
}

/// Render a self-contained HTML report (data embedded as JSON). The
/// template's `snapshot_labels` array is derived from each report's label.
pub fn render_html_report(snapshots: &[Report]) -> Result<String> {
    let wrapper = ReportWrapper {
        snapshot_labels: snapshots.iter().map(|r| r.label.as_str()).collect(),
        snapshots,
    };
    let json = escape_script_json(&to_string(&wrapper)?);
    Ok(TEMPLATE.replace("__DATA__", &json))
}

fn escape_script_json(json: &str) -> String {
    let mut escaped = String::with_capacity(json.len());
    for ch in json.chars() {
        match ch {
            '<' => escaped.push_str("\\u003C"),
            '>' => escaped.push_str("\\u003E"),
            '&' => escaped.push_str("\\u0026"),
            '\u{2028}' => escaped.push_str("\\u2028"),
            '\u{2029}' => escaped.push_str("\\u2029"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The escaping neutralizes the `</script>` break and `&` that a
    /// snapshot label or node name could otherwise smuggle into the page.
    #[test]
    fn embedded_json_is_safe_for_script_tag() {
        let escaped = escape_script_json("</script><script>alert(1)</script>&");

        assert!(!escaped.contains("</script>"));
        assert!(escaped.contains("\\u003C/script\\u003E"));
        assert!(escaped.contains("\\u0026"));
    }
}
