use crate::{
    config::{FASTCDC_AVG_CHUNK_SIZE, FASTCDC_MAX_CHUNK_SIZE, FASTCDC_MIN_CHUNK_SIZE},
    domain::{Chunk, ChunkRef},
};
use fastcdc::v2020::FastCDC;

pub fn split(data: &[u8]) -> Vec<Chunk> {
    FastCDC::new(
        data,
        FASTCDC_MIN_CHUNK_SIZE,
        FASTCDC_AVG_CHUNK_SIZE,
        FASTCDC_MAX_CHUNK_SIZE,
    )
    .map(|chunk| {
        let bytes = &data[chunk.offset..chunk.offset + chunk.length];
        Chunk::new(bytes.to_vec())
    })
    .collect()
}

pub fn truncate(chunk_refs: &[ChunkRef], new_size: u64) -> Vec<ChunkRef> {
    let mut result = Vec::new();
    let mut remaining = new_size;

    for chunk_ref in chunk_refs {
        if remaining == 0 {
            break;
        }

        if chunk_ref.size <= remaining {
            result.push(chunk_ref.clone());
            remaining -= chunk_ref.size;
        } else {
            result.push(ChunkRef {
                hash: chunk_ref.hash.clone(),
                size: remaining,
            });
            break;
        }
    }

    result
}
