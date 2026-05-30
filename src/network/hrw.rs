use sha2::{Digest, Sha256};

use crate::domain::ChunkHash;
use libp2p::PeerId;

pub fn hrw_score(hash: &ChunkHash, peer: &PeerId) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(hash.as_bytes());
    hasher.update(peer.to_bytes());
    let digest: [u8; 32] = hasher.finalize().into();
    let mut score = [0u8; 8];
    score.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(score)
}

pub fn top_peers(hash: &ChunkHash, members: &[PeerId], n: usize) -> Vec<PeerId> {
    let mut scored: Vec<(u64, PeerId)> = members
        .iter()
        .map(|peer| (hrw_score(hash, peer), *peer))
        .collect();
    scored.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then_with(|| b.1.to_bytes().cmp(&a.1.to_bytes()))
    });
    scored.truncate(n);
    scored.into_iter().map(|(_, peer)| peer).collect()
}
