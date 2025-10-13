#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
GUIDE_DIR="$ROOT_DIR"
DIST_DIR="$GUIDE_DIR/dist"
CHAPTERS_DIR="$GUIDE_DIR/chapters"
DIAGRAMS_DIR="$GUIDE_DIR/diagrams"
HEADER_TEX="$GUIDE_DIR/pandoc-header.tex"

usage() {
  echo "Usage: $(basename "$0") [html|pdf]" >&2
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1" >&2; exit 1; }
}

render_mermaid() {
  if compgen -G "$DIAGRAMS_DIR/*.mmd" > /dev/null; then
    echo "Rendering Mermaid diagrams..."
    require_cmd mmdc
    for f in "$DIAGRAMS_DIR"/*.mmd; do
      base="${f%.mmd}"
      mmdc -i "$f" -o "${base}.svg" --backgroundColor white --scale 1.2
    done
  else
    echo "No Mermaid diagrams found. Skipping render."
  fi
}

build_html() {
  require_cmd pandoc
  mkdir -p "$DIST_DIR"
  echo "Building HTML..."
  pandoc \
    "$CHAPTERS_DIR"/*.md \
    -o "$DIST_DIR/sstables-definitive-guide.html" \
    --from gfm \
    --toc --toc-depth=2 \
    --resource-path=".:$GUIDE_DIR"
  echo "HTML written to $DIST_DIR/sstables-definitive-guide.html"
}

build_pdf() {
  require_cmd pandoc
  mkdir -p "$DIST_DIR"
  echo "Building PDF..."
  pandoc \
    "$CHAPTERS_DIR"/*.md \
    -o "$DIST_DIR/sstables-definitive-guide.pdf" \
    --from gfm \
    --pdf-engine=xelatex \
    --toc --toc-depth=2 \
    --resource-path=".:$GUIDE_DIR" \
    -V geometry:margin=1in \
    -V mainfont="Noto Sans" -V monofont="Noto Sans Mono" \
    --include-in-header="$HEADER_TEX" \
    -V colorlinks=true 
  echo "PDF written to $DIST_DIR/sstables-definitive-guide.pdf"
}

main() {
  if [[ $# -lt 1 ]]; then
    usage; exit 1;
  fi
  render_mermaid
  case "$1" in
    html) build_html ;;
    pdf) build_pdf ;;
    *) usage; exit 1 ;;
  esac
}

main "$@"
