# decode_unsigned codegen fingerprint — the instruction idiom the annotate route matches on (#3445 AC1)

Binary: codegen-faithful build (release codegen, `debug=0`, `strip=none`) of the #3299
bare-scan worker. Toolchain rustc 1.97.1 (the repo pin, and the same one #3248 measured with).

`decode_unsigned` carries **no symbol of its own** in this binary — it is `#[inline]` and was
fully inlined at every hot call site, which IS the blind spot #3027 hit and this issue exists to
see through:

```
$ nm <binary> | grep -c decode_unsigned
0
```

The out-of-line `parse_vuint` nom adapter, however, still exists, and it inlines
`decode_unsigned` whole — so it is the reference specimen for what the idiom compiles to.
Disassembly below, annotated line-by-line against the source in
`cqlite-core/src/parser/vint.rs:40-77`.

```
00000000002907a0 <_RNvNtNtCslsIGQVo7ugv_11cqlite_core6parser4vint11parse_vuint>:
  2907a0:	41 57                	push   %r15
  2907a2:	41 56                	push   %r14
  2907a4:	41 55                	push   %r13
  2907a6:	41 54                	push   %r12
  2907a8:	53                   	push   %rbx
  2907a9:	48 83 ec 10          	sub    $0x10,%rsp
  2907ad:	48 85 d2             	test   %rdx,%rdx
  2907b0:	0f 84 b0 00 00 00    	je     290866 <_RNvNtNtCslsIGQVo7ugv_11cqlite_core6parser4vint11parse_vuint+0xc6>
  2907b6:	44 0f b6 36          	movzbl (%rsi),%r14d
  2907ba:	44 89 f0             	mov    %r14d,%eax
  2907bd:	f6 d0                	not    %al
  2907bf:	0f b6 c0             	movzbl %al,%eax
  2907c2:	bb 0f 00 00 00       	mov    $0xf,%ebx
  2907c7:	0f bd d8             	bsr    %eax,%ebx
  2907ca:	83 f3 07             	xor    $0x7,%ebx
  2907cd:	48 39 da             	cmp    %rbx,%rdx
  2907d0:	0f 86 90 00 00 00    	jbe    290866 <_RNvNtNtCslsIGQVo7ugv_11cqlite_core6parser4vint11parse_vuint+0xc6>
  2907d6:	48 85 db             	test   %rbx,%rbx
  2907d9:	0f 84 a8 00 00 00    	je     290887 <_RNvNtNtCslsIGQVo7ugv_11cqlite_core6parser4vint11parse_vuint+0xe7>
  2907df:	49 89 d5             	mov    %rdx,%r13
  2907e2:	49 89 ff             	mov    %rdi,%r15
  2907e5:	48 c7 44 24 08 00 00 	movq   $0x0,0x8(%rsp)
  2907ec:	00 00 
  2907ee:	49 89 f4             	mov    %rsi,%r12
  2907f1:	48 ff c6             	inc    %rsi
  2907f4:	48 8d 7c 24 08       	lea    0x8(%rsp),%rdi
  2907f9:	48 29 df             	sub    %rbx,%rdi
  2907fc:	48 83 c7 08          	add    $0x8,%rdi
  290800:	48 89 da             	mov    %rbx,%rdx
  290803:	ff 15 ff d2 2d 00    	call   *0x2dd2ff(%rip)        # 56db08 <memcpy@GLIBC_2.14>
  290809:	48 8b 44 24 08       	mov    0x8(%rsp),%rax
  29080e:	48 0f c8             	bswap  %rax
  290811:	41 81 fe ff 00 00 00 	cmp    $0xff,%r14d
  290818:	74 1d                	je     290837 <_RNvNtNtCslsIGQVo7ugv_11cqlite_core6parser4vint11parse_vuint+0x97>
  29081a:	b1 07                	mov    $0x7,%cl
  29081c:	28 d9                	sub    %bl,%cl
  29081e:	ba ff ff ff ff       	mov    $0xffffffff,%edx
  290823:	d3 e2                	shl    %cl,%edx
  290825:	f7 d2                	not    %edx
  290827:	41 21 d6             	and    %edx,%r14d
  29082a:	8d 0c dd 00 00 00 00 	lea    0x0(,%rbx,8),%ecx
  290831:	49 d3 e6             	shl    %cl,%r14
  290834:	4c 09 f0             	or     %r14,%rax
  290837:	4c 89 ea             	mov    %r13,%rdx
  29083a:	48 ff c3             	inc    %rbx
  29083d:	49 89 c6             	mov    %rax,%r14
  290840:	4c 89 ff             	mov    %r15,%rdi
  290843:	4c 89 e6             	mov    %r12,%rsi
  290846:	48 89 d0             	mov    %rdx,%rax
  290849:	48 29 d8             	sub    %rbx,%rax
  29084c:	72 46                	jb     290894 <_RNvNtNtCslsIGQVo7ugv_11cqlite_core6parser4vint11parse_vuint+0xf4>
  29084e:	48 01 de             	add    %rbx,%rsi
  290851:	48 89 77 08          	mov    %rsi,0x8(%rdi)
```

## The four fingerprint elements (what region identification keys on)

| # | source | instruction idiom | distinctiveness |
|---|---|---|---|
| F1 | `*input.first()` + len check | `movzbl (%rsi),%rNd` then `cmp`/`jbe` | low on its own |
| F2 | `first.leading_ones()` | **`not %al` ; `movzbl` ; `mov $0xf,%eXx` ; `bsr` ; `xor $0x7,%eXx`** | **very high** |
| F3 | `u64::from_be_bytes(be)` | **`bswap %rax`** (64-bit form) | **high** |
| F4 | `be[8-extra..].copy_from_slice(...)` | **`call *…memcpy@GLIBC`** | high, but shared with every other memcpy |

F2 is the anchor. `leading_ones()` on a `u8` lowers to `bsr` + `xor $0x7` (bit-index → leading-zero
count) with a `mov $0xf` predecessor supplying the `bsr`-undefined-on-zero default; that 5-instruction
run appears essentially nowhere else in this binary's hot path. F3 is the bit-assembly. **Region
identification therefore does NOT rest on the fingerprint alone** — the fingerprint is used to
CORROBORATE regions that DWARF line records independently attribute to `parser/vint.rs`, and the two
are reported against each other in `region-attribution.md`.

## Finding recorded here because it bears on any future lever, not on this issue's number

The multi-byte path issues a **dynamic-length `call memcpy@GLIBC`** (F4) rather than a fixed 8-byte
load-and-mask. #1638 J4's claim is "no per-byte index loop", which remains true; the loop was
replaced by a libc call whose length is a runtime value. Whether that call is ever reached in a
measured scan is an EMPIRICAL question about the corpus's VInt width distribution, answered by
measurement in `vint-width-distribution.md` — not assumed here in either direction.
