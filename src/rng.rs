//! A tiny deterministic pseudo-random generator (xorshift64*).
//!
//! Quorum never uses wall-clock randomness. Every timeout the simulation
//! picks comes from this seeded stream, so a run with the same seed and
//! the same inputs always produces the same election, every time.

#[derive(Clone, Debug)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        // xorshift64* requires a non-zero state.
        let state = if seed == 0 { 0x9E3779B97F4A7C15 } else { seed };
        Rng { state }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    /// Returns a value in `[min, max]` inclusive.
    pub fn range(&mut self, min: u64, max: u64) -> u64 {
        if max <= min {
            return min;
        }
        let span = max - min + 1;
        min + (self.next_u64() % span)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..50 {
            assert_eq!(a.range(150, 300), b.range(150, 300));
        }
    }

    #[test]
    fn stays_in_range() {
        let mut r = Rng::new(7);
        for _ in 0..1000 {
            let v = r.range(150, 300);
            assert!((150..=300).contains(&v));
        }
    }
}
