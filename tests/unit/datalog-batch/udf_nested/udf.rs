/// Normalize to 0-1000 scale: value * 1000 / (value + scale_factor)
pub fn normalize(value: i32, scale_factor: i32) -> i32 {
    if scale_factor <= 0 { return 0; }
    let v = if value < 0 { 0 } else { value };
    (v as i64 * 1000 / (v as i64 + scale_factor as i64)) as i32
}

/// Classify into buckets: 0-249->1, 250-499->2, 500-749->3, 750+->4
pub fn classify(normalized_value: i32) -> i32 {
    match normalized_value {
        v if v < 250 => 1,
        v if v < 500 => 2,
        v if v < 750 => 3,
        _ => 4,
    }
}

/// Weighted blend: (a * w + b * (100 - w)) / 100
pub fn blend(a: i32, b: i32, weight: i32) -> i32 {
    let w = if weight < 0 { 0 } else if weight > 100 { 100 } else { weight };
    (a as i64 * w as i64 + b as i64 * (100 - w) as i64) as i32 / 100
}

/// Clamp to [lo, hi]
pub fn clamp(value: i32, lo: i32, hi: i32) -> i32 {
    if value < lo { lo } else if value > hi { hi } else { value }
}

/// Absolute difference
pub fn abs_diff(a: i32, b: i32) -> i32 {
    (a - b).abs()
}
