#!/bin/bash
# Phase 5: build damaged COPIES under /dmg/<case>/fr/<table-dir>/ (never /snap, never hard links). Each case holds the damaged generation
# plus one healthy neighbour generation. Every case writes /dmg/<case>/damage.txt (what, offset, size) and expected.txt (from the CLEAN copy).
# runs in the oracle container (shared PVC).
set -e
SNAP=/snap/db0/fr
mk() { # case fmt table gen healthy
  local c=$1 fmt=$2 tbl=$3 g=$4 h=$5; local src=$(ls -d $SNAP/$tbl-*); local d=/dmg/$c/fr/$(basename $src)
  rm -rf /dmg/$c; mkdir -p $d; chmod -R u+w /dmg/$c 2>/dev/null || true
  for gg in $g $h; do for f in $src/$gg-*; do cp --no-preserve=mode $f $d/; done; done
  echo $d
}
flip() { local f=$1 off=$2; local b=$(dd if=$f bs=1 skip=$off count=1 2>/dev/null | od -An -tu1 | tr -d ' '); local nb=$(( (b ^ 255) & 255 )); printf "$(printf '\\%03o' $nb)" | dd of=$f bs=1 seek=$off conv=notrunc 2>/dev/null; echo "flipped byte at offset $off: $b -> $nb"; }
trunc() { truncate -s $2 $1; echo "truncated $(basename $1) to $2 bytes"; }
for fmt in big bti; do
  if [ $fmt = big ]; then tbl=t_tomb; g=nb-6-big; h=nb-5-big; key=8722; else tbl=t_static; g=da-10-bti; h=da-11-bti; key="pk86127:6"; fi
  p=$fmt
  S=$(stat -c %s $SNAP/$tbl-*/$g-Data.db)
  case_() { local c=$1; shift; d=$(mk $c $fmt $tbl $g $h); eval "$@" > /dmg/$c/damage.txt 2>&1; echo "$c: $(cat /dmg/$c/damage.txt | tr '\n' ';')"; (cd $d && sha256sum * > /dmg/$c/before.sha256); }
  case_ D10-$p-control  "echo none"
  case_ D1a-$p-stats-half   "trunc \$d/$g-Statistics.db \$(( \$(stat -c %s \$d/$g-Statistics.db) / 2 ))"
  case_ D1b-$p-stats-zero   "trunc \$d/$g-Statistics.db 0"
  case_ D1c-$p-stats-4b     "trunc \$d/$g-Statistics.db 4"
  [ $fmt = big ] && case_ D2-$p-filter-empty "trunc \$d/$g-Filter.db 0"
  case_ D3-$p-data-flip     "flip \$d/$g-Data.db $(( S / 2 ))"
  case_ D4-$p-data-trunc4k  "trunc \$d/$g-Data.db $(( S - 4096 ))"
  if [ $fmt = big ]; then case_ D5-$p-index-flip "flip \$d/$g-Index.db 100"; else case_ D5-$p-partitions-flip "flip \$d/$g-Partitions.db 100"; fi
  case_ D6-$p-digest-edit   "printf 1234567890 > \$d/$g-Digest.crc32; echo 'Digest.crc32 replaced with 1234567890'"
  case_ D7a-$p-toc-missing-file "rm \$d/$g-CompressionInfo.db; echo 'removed CompressionInfo.db (still listed in TOC.txt)'"
  case_ D7b-$p-toc-extra-file   "cp \$d/$g-Statistics.db \$d/$g-Extra.db; echo 'added unlisted $g-Extra.db'"
  [ $fmt = big ] && case_ D8-$p-summary-trunc "trunc \$d/$g-Summary.db 100"
  case_ D9-$p-compinfo-trunc "trunc \$d/$g-CompressionInfo.db \$(( \$(stat -c %s \$d/$g-CompressionInfo.db) / 2 ))"
done
