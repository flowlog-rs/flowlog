/// Bandwidth cost with congestion penalty: if weight > 10,
/// add (weight - 10)^2 to model congestion.
pub fn bw_cost(weight: i32) -> i32 {
    if weight <= 10 {
        weight
    } else {
        let over = weight - 10;
        weight + over * over
    }
}

/// Classify weight into tiers: 1-10 -> 1, 11-20 -> 2, 21+ -> 3
pub fn tier(weight: i32) -> i32 {
    if weight <= 10 { 1 }
    else if weight <= 20 { 2 }
    else { 3 }
}
