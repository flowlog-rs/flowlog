/// 3-tier late penalty: 25/day (1-3), 50/day (4-7), 100/day (8+)
pub fn penalty(days_overdue: i32) -> i32 {
    if days_overdue <= 0 {
        return 0;
    }
    let t1 = std::cmp::min(days_overdue, 3) * 25;
    let t2 = std::cmp::max(0, std::cmp::min(days_overdue, 7) - 3) * 50;
    let t3 = std::cmp::max(0, days_overdue - 7) * 100;
    t1 + t2 + t3
}

/// 8% tax on price (integer division)
pub fn tax(price: i32) -> i32 {
    price * 8 / 100
}
