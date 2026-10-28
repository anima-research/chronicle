pub(super) const MIN_CHUNK: usize = 2 * 1024;
pub(super) const MAX_CHUNK: usize = 64 * 1024;
const CUT_BITS: u32 = 13;

const GEAR: [u64; 256] = gear();

const fn gear() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut state: u64 = 0x6368_726f_6e69_636c;
    let mut i = 0;
    while i < 256 {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        table[i] = z ^ (z >> 31);
        i += 1;
    }
    table
}

pub(super) fn chunk_ends(content: &[u8]) -> Vec<usize> {
    let mut ends = Vec::new();
    let mut start = 0;
    while start < content.len() {
        start += cut(&content[start..]);
        ends.push(start);
    }
    ends
}

fn cut(rest: &[u8]) -> usize {
    if rest.len() <= MIN_CHUNK {
        return rest.len();
    }
    let end = rest.len().min(MAX_CHUNK);
    let mut rolling: u64 = 0;
    for (i, &byte) in rest[MIN_CHUNK..end].iter().enumerate() {
        rolling = (rolling << 1).wrapping_add(GEAR[usize::from(byte)]);
        if rolling >> (64 - CUT_BITS) == 0 {
            return MIN_CHUNK + i + 1;
        }
    }
    end
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect()
    }

    #[test]
    fn chunks_cover_the_content_in_order_within_their_bounds() {
        for len in [
            0,
            1,
            MIN_CHUNK,
            MIN_CHUNK + 1,
            MAX_CHUNK,
            MAX_CHUNK + 1,
            1 << 20,
        ] {
            for content in [vec![0u8; len], pseudo_random(len, 7)] {
                let ends = chunk_ends(&content);
                let mut start = 0;
                for (i, &end) in ends.iter().enumerate() {
                    let size = end - start;
                    assert!(
                        size > 0 && size <= MAX_CHUNK,
                        "chunk {i} of {len} bytes is {size}"
                    );
                    if i + 1 < ends.len() {
                        assert!(size >= MIN_CHUNK, "chunk {i} of {len} bytes is {size}");
                    }
                    start = end;
                }
                assert_eq!(start, len);
            }
        }
    }

    #[test]
    fn a_change_moves_only_the_cuts_near_it() {
        let before = pseudo_random(1 << 20, 11);
        let mut after = before.clone();
        after.splice(500_000..500_000, pseudo_random(300, 12));
        let pieces = |content: &[u8]| {
            let mut start = 0;
            chunk_ends(content)
                .into_iter()
                .map(|end| {
                    let piece = content[start..end].to_vec();
                    start = end;
                    piece
                })
                .collect::<Vec<_>>()
        };
        let (old, new) = (pieces(&before), pieces(&after));
        let changed = new.iter().filter(|piece| !old.contains(piece)).count();
        assert!(changed <= 3, "{changed} of {} chunks are new", new.len());
    }
}
