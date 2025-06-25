use std::ops::Range;

use tokio::time;

pub struct Config {
    pub tick_duration: time::Duration,
    pub election_timeout_range: Range<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            tick_duration: time::Duration::from_millis(100),
            election_timeout_range: 10..20,
        }
    }
}
