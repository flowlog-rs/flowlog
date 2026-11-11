//! Process-wide runtime initialization.

use std::sync::Once;

/// Initializes enabled runtime components once per process.
///
/// Call during startup, before performing runtime I/O or starting workers.
/// Initialization depends on the enabled features; repeated calls have no
/// effect.
pub fn initialize() {
    static INITIALIZED: Once = Once::new();
    INITIALIZED.call_once(|| {
        #[cfg(feature = "sqlite")]
        {
            // Memory statistics serialize SQLite allocations through a global
            // mutex. FlowLog does not use them, and concurrent schema loading
            // benefits from disabling them before SQLite initializes.
            // SAFETY: SQLITE_CONFIG_MEMSTATUS takes one int argument. Once
            // serializes these calls; startup precedes other SQLite calls.
            // SQLite refuses configuration after initialization, leaving the
            // host's settings unchanged in that case.
            unsafe {
                rusqlite::ffi::sqlite3_config(rusqlite::ffi::SQLITE_CONFIG_MEMSTATUS, 0_i32);
            }
        }
    });
}
