pub fn is_prime(n: i32) -> bool {
    // Primality check with early exits and loop
    let val = if n < 0 { -n } else { n };
    if val < 2 {
        return false;
    }
    if val == 2 || val == 3 {
        return true;
    }
    if val % 2 == 0 || val % 3 == 0 {
        return false;
    }
    let mut i = 5;
    while i * i <= val {
        if val % i == 0 || val % (i + 2) == 0 {
            return false;
        }
        i += 6;
    }
    true
}
