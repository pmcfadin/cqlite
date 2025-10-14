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
      # Also produce PDF for LaTeX inclusion fallback
      mmdc -i "$f" -o "${base}.pdf" --backgroundColor white --scale 1.2
      # Crop PDF whitespace if pdfcrop is available to avoid huge margins
      if command -v pdfcrop >/dev/null 2>&1; then
        cropped="${base}.cropped.pdf"
        pdfcrop --margins '5' "${base}.pdf" "$cropped" >/dev/null 2>&1 || true
        if [ -f "$cropped" ]; then mv "$cropped" "${base}.pdf"; fi
      fi
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
    --resource-path=".:$GUIDE_DIR:$DIAGRAMS_DIR" \
    --default-image-extension=svg
  echo "HTML written to $DIST_DIR/sstables-definitive-guide.html"
}

build_pdf() {
  require_cmd pandoc
  mkdir -p "$DIST_DIR"
  echo "Building PDF..."
  # Prepare temp chapters with PDF image links
  TMP_CHAPTERS_DIR=$(mktemp -d)
  cp "$CHAPTERS_DIR"/*.md "$TMP_CHAPTERS_DIR"/
  # Rewrite diagram image links to prefer .pdf for LaTeX build
  # macOS sed requires an empty string after -i for in-place edits
  sed -E -i '' 's#\]\(\.{2}/diagrams/([^)]+)\.svg\)#](../diagrams/\1.pdf)#g' "$TMP_CHAPTERS_DIR"/*.md
  sed -E -i '' 's#\]\(\.{2}/diagrams/([^)\.]+)\)#](../diagrams/\1.pdf)#g' "$TMP_CHAPTERS_DIR"/*.md
  sed -E -i '' 's#\]\(diagrams/([^)]+)\.svg\)#](diagrams/\1.pdf)#g' "$TMP_CHAPTERS_DIR"/*.md
  sed -E -i '' 's#\]\(diagrams/([^)\.]+)\)#](diagrams/\1.pdf)#g' "$TMP_CHAPTERS_DIR"/*.md

  pandoc \
    "$TMP_CHAPTERS_DIR"/*.md \
    -o "$DIST_DIR/sstables-definitive-guide.pdf" \
    --from gfm \
    --pdf-engine=xelatex \
    --toc --toc-depth=2 \
    --resource-path=".:$GUIDE_DIR:$DIAGRAMS_DIR" \
    -V geometry:margin=1in \
    -V mainfont="Helvetica Neue" -V monofont="Menlo" \
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
