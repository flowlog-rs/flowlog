use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Simple UDF hash function for SIP key projection.
///
/// This is intended to be declared in Datalog as:
/// `.extern fn hash(x: int32) -> int64`
pub fn hash(x: i64) -> i64 {
    let mut hasher = DefaultHasher::new();
    x.hash(&mut hasher);
    hasher.finish() as i64
}
