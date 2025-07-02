#!/usr/bin/env bash

# CSTeX Compile Script - Phase 2 Enhanced Integration
# ===================================================
#
# Intelligent LaTeX compiler that automatically injects CSF metadata and
# provenance links during compilation.
#
# This script implements the "Zero-Configuration Workflow" paradigm by:
# 1. Pre-processing LaTeX documents to inject CSF metadata
# 2. Compiling with enhanced composable.sty package
# 3. Generating dashboard URLs for each artifact
# 4. Handling both automatic and manual CSF commands

set -euo pipefail

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT=""      # Root of the git repository, contains flake.nix
PROJECT_ROOT=""   # Root of the CSF project, contains composable.toml
INPUT_FILE_DIR="" # Directory of the input LaTeX file
TEMP_DIR=""
CLEANUP_FILES=()

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[CSTeX]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[CSTeX]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[CSTeX]${NC} $1"
}

log_error() {
    echo -e "${RED}[CSTeX]${NC} $1" >&2
}

# Cleanup function
cleanup() {
    if [[ -n "$TEMP_DIR" && -d "$TEMP_DIR" ]]; then
        rm -rf "$TEMP_DIR"
    fi
    for file in "${CLEANUP_FILES[@]}"; do
        [[ -f "$file" ]] && rm -f "$file"
    done
}

trap cleanup EXIT

# Help function
show_help() {
    cat << EOF
CSTeX - Intelligent LaTeX Compiler with Automatic CSF Integration

USAGE:
    cstex-compile [OPTIONS] <latex-file>

OPTIONS:
    -o, --output FILE       Output PDF file (default: same name as input)
    -e, --engine ENGINE     LaTeX engine (pdflatex, xelatex, lualatex)
    -w, --watch            Watch mode - recompile on changes
    -p, --preview          Open PDF after compilation
    -d, --dashboard        Generate dashboard after compilation
    -v, --verbose          Verbose output
    -h, --help             Show this help message

EXAMPLES:
    cstex-compile paper.tex                    # Basic compilation
    cstex-compile paper.tex -o output.pdf     # Custom output name
    cstex-compile paper.tex -w -p              # Watch mode with preview
    cstex-compile paper.tex -d                 # Generate dashboard

This is part of the CSF Enhanced Integration Paradigm, enabling zero-configuration
computational transparency in scientific documents.
EOF
}

# Parse command line arguments
LATEX_FILE=""
OUTPUT_FILE=""
LATEX_ENGINE="latexmk"
WATCH_MODE=false
PREVIEW_MODE=false
DASHBOARD_MODE=false
VERBOSE=false
CONFIG_FILE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        -e|--engine)
            LATEX_ENGINE="$2"
            shift 2
            ;;
        -w|--watch)
            WATCH_MODE=true
            shift
            ;;
        -p|--preview)
            PREVIEW_MODE=true
            shift
            ;;
        -d|--dashboard)
            DASHBOARD_MODE=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        -*)
            log_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
        *)
            if [[ -z "$LATEX_FILE" ]]; then
                LATEX_FILE="$1"
            else
                log_error "Multiple input files not supported"
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate input
if [[ -z "$LATEX_FILE" ]]; then
    log_error "No LaTeX file specified"
    echo "Use --help for usage information"
    exit 1
fi

if [[ ! -f "$LATEX_FILE" ]]; then
    log_error "LaTeX file not found: $LATEX_FILE"
    exit 1
fi

# --- Path Resolution ---
# Get the absolute path to the input file and its directory
INPUT_FILE_ABS="$(cd "$(dirname "$LATEX_FILE")" && pwd)/$(basename "$LATEX_FILE")"
INPUT_FILE_DIR="$(dirname "$INPUT_FILE_ABS")"

# Find repository root by looking for flake.nix, starting from the input file's directory
CWD="$INPUT_FILE_DIR"
while [[ "$CWD" != "" && ! -f "$CWD/flake.nix" ]]; do
  CWD=${CWD%/*}
done
if [[ -z "$CWD" ]]; then
    log_error "Could not find repository root (containing flake.nix) from the input file's location."
    exit 1
fi
REPO_ROOT="$CWD"
log_info "Repository root identified: $REPO_ROOT"

# If a config file is provided, use its directory as the project root.
# Otherwise, search for flake.nix to determine the project root.
if [[ -n "$CONFIG_FILE" ]]; then
    PROJECT_ROOT="$(cd "$(dirname "$CONFIG_FILE")" && pwd)"
    log_info "Project root identified from config file: $PROJECT_ROOT"
else
    CWD="$INPUT_FILE_DIR"
    while [[ "$CWD" != "" && ! -f "$CWD/flake.nix" ]]; do
      CWD=${CWD%/*}
    done
    if [[ -z "$CWD" ]]; then
        log_error "Could not find project root (containing flake.nix) from the input file's location."
        exit 1
    fi
    PROJECT_ROOT="$CWD"
    log_info "Project root identified by flake.nix: $PROJECT_ROOT"
fi

# Set default output file relative to the original file's location
if [[ -z "$OUTPUT_FILE" ]]; then
    OUTPUT_FILE="${INPUT_FILE_DIR}/$(basename "${LATEX_FILE%.*}").pdf"
fi

# Check for required tools
check_dependencies() {
    local missing_deps=()
    
    command -v nix >/dev/null || missing_deps+=("nix")
    command -v python3 >/dev/null || missing_deps+=("python3")
    
    if [[ ${#missing_deps[@]} -gt 0 ]]; then
        log_error "Missing dependencies: ${missing_deps[*]}"
        log_error "Please install Nix to use texMini for LaTeX compilation"
        exit 1
    fi
}

# Pre-process LaTeX document with CSF metadata injection
preprocess_document() {
    local input_file="$1"
    local output_file="$2"
    
    log_info "Pre-processing LaTeX document for CSF integration..."
    
    # Create temporary directory
    TEMP_DIR=$(mktemp -d)
    
    # Run metadata processor, ensuring all paths are absolute
    
    # Step 1: Extract values from the provenance log
    log_info "Extracting values from provenance log..."
    extract-values.py --config "$CONFIG_FILE" --project-root "$PROJECT_ROOT" || log_warning "Value extraction failed, continuing with fallback values"
    
    # Step 3: Process metadata and enhance document
    # Pass the absolute path to the input file.
    process-metadata.py "$INPUT_FILE_ABS" -o "$output_file" --config "$CONFIG_FILE" --project-root "$PROJECT_ROOT"
    
    # Add enhanced document to cleanup
    CLEANUP_FILES+=("$output_file")
}

# Compile LaTeX document using texMini
compile_latex() {
    local input_file="$1"
    local output_file="$2"
    
    log_info "Compiling LaTeX document with texMini..."
    
    # The texMini environment is now in the PATH, so we can call latexmk directly.
    # We must run latexmk from the input file's directory so it can find figures, etc.
    # We call it via `nix run` to ensure we use the version from the texMini flake.
    (
      cd "$INPUT_FILE_DIR"
      # Per user instruction, we now use `nix run` to invoke texMini.
      # We use the `latexmk` app from texMini to clean the directory first.
      nix run github:composable-science/texMini#latexmk -- -C "$input_file"
      # Then we compile.
      nix run github:composable-science/texMini -- "$input_file"
    )
    
    if [[ $? -ne 0 ]]; then
        log_error "LaTeX compilation failed."
        log_error "Check that your LaTeX document is valid."
        exit 1
    fi
    
    # Check if PDF was generated (texMini auto-names output)
    local expected_pdf="${input_file%.*}.pdf"
    if [[ -f "$expected_pdf" ]]; then
        # If output file is different, move it
        if [[ "$expected_pdf" != "$output_file" ]]; then
            mv "$expected_pdf" "$output_file"
        fi
        log_success "PDF generated: $output_file"
    else
        log_error "PDF was not generated. Check LaTeX compilation logs."
        exit 1
    fi
}

# Generate CSF dashboard
generate_dashboard() {
    log_info "Generating CSF dashboard..."
    
    # Check if we're in a CSF project
    if [[ ! -f "$PROJECT_ROOT/composable.toml" ]]; then
        log_error "No composable.toml found in the project root: $PROJECT_ROOT"
        exit 1
    fi
    
    # Use the dashboard generator from the flake, running from the project root
    if command -v nix >/dev/null; then
        (
          cd "$PROJECT_ROOT"
          nix run .#dashboard -- --no-open
        )
    else
        log_warning "Nix not available, skipping dashboard generation"
    fi
}

# Watch mode implementation
watch_mode() {
    local input_file="$1"
    local output_file="$2"
    
    log_info "Starting watch mode for $input_file"
    log_info "Press Ctrl+C to stop watching"
    
    # Initial compilation
    compile_document "$input_file" "$output_file"
    
    # Watch for changes
    if command -v fswatch >/dev/null; then
        fswatch -o "$input_file" | while read num; do
            log_info "File changed, recompiling..."
            compile_document "$input_file" "$output_file"
        done
    elif command -v inotifywait >/dev/null; then
        while inotifywait -e modify "$input_file" >/dev/null 2>&1; do
            log_info "File changed, recompiling..."
            compile_document "$input_file" "$output_file"
        done
    else
        log_warning "No file watcher available (fswatch or inotifywait)"
        log_warning "Falling back to polling mode"
        
        local last_modified=$(stat -c %Y "$input_file" 2>/dev/null || stat -f %m "$input_file")
        while true; do
            sleep 2
            local current_modified=$(stat -c %Y "$input_file" 2>/dev/null || stat -f %m "$input_file")
            if [[ "$current_modified" != "$last_modified" ]]; then
                log_info "File changed, recompiling..."
                compile_document "$input_file" "$output_file"
                last_modified="$current_modified"
            fi
        done
    fi
}

# Open PDF in viewer
open_preview() {
    local pdf_file="$1"
    
    if [[ ! -f "$pdf_file" ]]; then
        log_warning "PDF file not found: $pdf_file"
        return
    fi
    
    log_info "Opening PDF preview..."
    
    if command -v open >/dev/null; then
        # macOS
        open "$pdf_file"
    elif command -v xdg-open >/dev/null; then
        # Linux
        xdg-open "$pdf_file"
    elif command -v start >/dev/null; then
        # Windows (WSL)
        start "$pdf_file"
    else
        log_warning "No PDF viewer command found"
        log_info "Please open manually: $pdf_file"
    fi
}

# Main compilation function
compile_document() {
    local input_file="$1"
    local output_file="$2"
    
    # Create enhanced document with CSF metadata
    local enhanced_file="${input_file%.*}_enhanced.tex"
    preprocess_document "$input_file" "$enhanced_file"
    
    # Compile the enhanced document
    compile_latex "$enhanced_file" "$output_file"
    
    # Generate dashboard if requested
    if [[ "$DASHBOARD_MODE" == true ]]; then
        generate_dashboard
    fi
    
    # Open preview if requested
    if [[ "$PREVIEW_MODE" == true ]]; then
        open_preview "$output_file"
    fi
}

# Main execution
main() {
    log_info "CSTeX Intelligent LaTeX Compiler"
    log_info "Processing: $LATEX_FILE"
    
    # Check dependencies
    check_dependencies
    
    # Handle watch mode
    if [[ "$WATCH_MODE" == true ]]; then
        watch_mode "$LATEX_FILE" "$OUTPUT_FILE"
    else
        compile_document "$LATEX_FILE" "$OUTPUT_FILE"
        log_success "Compilation complete!"
        
        if [[ "$DASHBOARD_MODE" == true ]]; then
            log_info "Dashboard available at: dashboard/index.html"
        fi
    fi
}

# Run main function
main "$@"