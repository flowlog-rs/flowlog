pub fn transform(x: i32, y: i32) -> i32 {
    // Iterative mixing function: combine two values
    // with bit manipulation and conditional logic
    let mut result = x;
    let mut acc = y.wrapping_abs();
    while acc > 0 {
        if acc % 2 == 0 {
            result = result.wrapping_add(acc);
        } else {
            result = result.wrapping_mul(3).wrapping_add(1);
        }
        acc /= 2;
    }
    if result < 0 { -result } else { result }
}
