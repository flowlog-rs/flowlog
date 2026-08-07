/// String length (string → int)
pub fn strlen(s: String) -> i32 {
    s.len() as i32
}

/// Integer to label (int → string)
pub fn to_label(n: i32) -> String {
    if n < 50 { format!("low_{}", n) }
    else if n < 150 { format!("mid_{}", n) }
    else { format!("high_{}", n) }
}

/// Format with tag (int, string → string)
pub fn tag(id: i32, name: String) -> String {
    format!("{}:{}", id, name)
}

/// Score from name length and price (string, int → int)
pub fn score(name: String, price: i32) -> i32 {
    name.len() as i32 * 10 + price
}
