use std::time::Duration;

pub const FASTCDC_MIN_CHUNK_SIZE: usize = 1024;
pub const FASTCDC_AVG_CHUNK_SIZE: usize = 512 * 1024;
pub const FASTCDC_MAX_CHUNK_SIZE: usize = 1024 * 1024;

pub const SWARM_CHANNEL_CAPACITY: usize = 256;
pub const DISPATCH_TICK_INTERVAL: Duration = Duration::from_millis(250);

pub const MAX_FETCH_TARGETS: usize = 3;
pub const CHUNK_FETCH_BACKOFF: Duration = Duration::from_secs(5);

pub const NETWORK_PROTOCOL_NAME: &str = "/edfs/dev";
pub const NETWORK_GOSSIP_TOPIC: &str = "edfs-metadata";
pub const NETWORK_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub const MAX_GOSSIP_MESSAGE_BYTES: usize = 2 * 1024 * 1024;

pub const REPLICATION_REBALANCE_INTERVAL: Duration = Duration::from_secs(5);
pub const REPLICATION_EVICTION_INTERVAL: Duration = Duration::from_secs(30);
pub const MAX_HASHES_PER_PROBE: usize = 256;
pub const MAX_FETCHES_PER_REBALANCE: usize = 64;

pub const FUSE_BLOCK_SIZE_BYTES: u32 = 512;
pub const FUSE_MAX_NAME_LENGTH: u32 = 255;
pub const MOUNT_STATUS_POLL_INTERVAL: Duration = Duration::from_millis(500);
