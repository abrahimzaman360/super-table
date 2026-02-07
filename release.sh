#!/bin/bash
set -e

# SuperTable Release Script
# Usage: ./release.sh <version> [--publish]

VERSION=$1
PUBLISH=$2

if [ -z "$VERSION" ]; then
    echo "Usage: ./release.sh <version> [--publish]"
    exit 1
fi

echo "--------------------------------------------------"
echo "🚀 Preparing SuperTable Release: v$VERSION"
echo "--------------------------------------------------"

# 1. Update Rust Workspace Version
echo "📦 Phase 1: Updating Rust workspace version..."
# Use sed to update version in root Cargo.toml (Linux sed)
sed -i "s/^version = \".*\"/version = \"$VERSION\"/" Cargo.toml
echo "✅ dependency versions updated."

# 2. Update Python Version (pyproject.toml)
echo "🐍 Phase 2: Updating Python version..."
sed -i "s/^version = \".*\"/version = \"$VERSION\"/" superbindings/pyproject.toml
sed -i "s/__version__ = \".*\"/__version__ = \"$VERSION\"/" superbindings/python/supertable/__init__.py
echo "✅ Python versions updated."

# 3. Verification
echo "🔍 Phase 3: Verifying build..."
cargo check
cd superbindings && maturin build --release && cd ..
echo "✅ Build verification passed."

# 4. Git Tag & Publish
if [ "$PUBLISH" == "--publish" ]; then
    echo "📣 Phase 4: Publishing..."
    
    # 4a. Git Tag
    git tag -a "v$VERSION" -m "Release v$VERSION"
    # git push origin "v$VERSION" # User can push manually
    echo "✅ Git tag created."

    # 4b. Publish Crates
    echo "📤 Publishing Rust crates..."
    # Publish core first
    cargo publish -p supertable-core
    # Publish bindings (Maturin handles this usually, or cargo publish if pure rust)
    # But supertable-python is cdylib, so we use maturin for PyPI
    
    # 4c. Publish to PyPI
    echo "📤 Publishing to PyPI..."
    cd superbindings
    maturin publish
    cd ..
    
    echo "🎉 Release v$VERSION published successfully!"
else
    echo "--------------------------------------------------"
    echo "✅ Release preparation complete (Dry Run)."
    echo "To publish, run: ./release.sh $VERSION --publish"
    echo "--------------------------------------------------"
fi
