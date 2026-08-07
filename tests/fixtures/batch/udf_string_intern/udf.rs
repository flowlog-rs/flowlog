pub fn upper(s: String) -> String {
    s.to_uppercase()
}

pub fn reverse(s: String) -> String {
    s.chars().rev().collect()
}

pub fn join_str(a: String, sep: String, b: String) -> String {
    format!("{}{}{}", a, sep, b)
}

pub fn strlen(s: String) -> i32 {
    s.len() as i32
}

pub fn take_n(s: String, n: i32) -> String {
    s.chars().take(n as usize).collect()
}

pub fn starts_with(s: String, prefix: String) -> i32 {
    if s.starts_with(&prefix) { 1 } else { 0 }
}

pub fn replace_str(s: String, from: String, to: String) -> String {
    s.replace(&from, &to)
}
