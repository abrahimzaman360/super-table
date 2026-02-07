#!/bin/bash
set -e

# SuperTable Release Script
# This script performs a dry-run check of all packages before publishing.

echo "--------------------------------------------------"
echo "🚀 Starting SuperTable Release Process (Dry Run)"
echo "--------------------------------------------------"

# 1. Rust Crates Check
echo "📦 Phase 1: Checking Rust crates..."
cargo check
echo "✅ Rust crates check passed."

# 2. Python Bindings (Maturin)
echo "🐍 Phase 2: Building Python wheels (dry run)..."
cd superbindings
if command -v maturin &> /dev/null; then
    maturin build --release
    echo "✅ Python build passed."
else
    echo "⚠️  maturin not found, skipping Python build check."
fi
cd ..

# 4. Final Validation
echo "--------------------------------------------------"
echo "✅ All checks completed!"
echo "Next steps:"
echo "1. cargo publish -p supertable-core"
echo "2. maturin publish (from superbindings)"
echo "--------------------------------------------------"
