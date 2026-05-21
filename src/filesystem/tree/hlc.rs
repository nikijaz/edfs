use std::time::{SystemTime, UNIX_EPOCH};

const LOGICAL_BITS: u32 = 16;
const LOGICAL_MASK: u64 = (1u64 << LOGICAL_BITS) - 1;

#[derive(Default)]
pub struct HybridLogicalClock {
    physical: u64,
    logical: u64,
}

impl HybridLogicalClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn next(&mut self) -> u64 {
        let now_physical = Self::now();
        if now_physical > self.physical {
            self.physical = now_physical;
            self.logical = 0;
        } else {
            self.logical += 1;
        }
        self.pack()
    }

    pub fn observe(&mut self, remote: u64) {
        let now = Self::now();
        let remote_physical = remote >> LOGICAL_BITS;
        let remote_logical = remote & LOGICAL_MASK;

        let physical = now.max(self.physical).max(remote_physical);
        let logical = match (physical == self.physical, physical == remote_physical) {
            (true, true) => self.logical.max(remote_logical),
            (true, false) => self.logical,
            (false, true) => remote_logical,
            (false, false) => 0,
        };

        self.physical = physical;
        self.logical = logical;
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn pack(&self) -> u64 {
        (self.physical << LOGICAL_BITS) | (self.logical & LOGICAL_MASK)
    }
}
