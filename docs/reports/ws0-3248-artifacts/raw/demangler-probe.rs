use std::hint::black_box;
#[inline(never)]
pub fn spin_marker_alpha(n: u64) -> u64 { let mut a=0u64; for i in 0..n { a=black_box(a.wrapping_add(black_box(i)^0x9e37)); } a }
fn main(){ let mut t=0u64; for _ in 0..300 { t=t.wrapping_add(spin_marker_alpha(black_box(1_000_000))); } println!("{}",black_box(t)); }
