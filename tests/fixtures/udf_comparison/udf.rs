/// Quadratic cost: base + duration^2 * 10
pub fn cost(base: i32, duration: i32) -> i32 {
    base + duration * duration * 10
}

/// Risk score: hash-like combination of two ints, range 0-999
pub fn risk(a: i32, b: i32) -> i32 {
    ((a as i64 * 31 + b as i64 * 17) % 1000) as i32
}
