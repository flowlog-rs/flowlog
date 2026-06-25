//! Report rendering (HTML).

use crate::Result;
use crate::view::ReportData;

use serde::Serialize;
use serde_json::to_string;

const TEMPLATE: &str = include_str!("../templates/report.html");

#[derive(Serialize)]
struct ReportWrapper<'a> {
    snapshot_labels: &'a [String],
    snapshots: &'a [ReportData],
}

/// Render a self-contained HTML report (data embedded as JSON).
pub fn render_html_report(labels: &[String], snapshots: &[ReportData]) -> Result<String> {
    let wrapper = ReportWrapper {
        snapshot_labels: labels,
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

    #[test]
    fn embedded_json_is_safe_for_script_tag() {
        let labels = vec!["</script><script>alert(1)</script>&".to_string()];
        let html = render_html_report(&labels, &[]).expect("render should succeed");

        assert!(!html.contains("</script><script>alert(1)</script>&"));
        assert!(html.contains("\\u003C/script\\u003E"));
        assert!(html.contains("\\u0026"));
    }
}
