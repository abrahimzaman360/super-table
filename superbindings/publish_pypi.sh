#!/bin/bash
# SuperTable PyPI Publishing Script
# Usage: ./publish_pypi.sh [--test]

set -e

echo "🐍 SuperTable PyPI Publisher"
echo "============================"

# Check for test flag
TEST_PYPI=""
if [[ "$1" == "--test" ]]; then
    TEST_PYPI="--repository testpypi"
    echo "📦 Publishing to TestPyPI"
else
    echo "📦 Publishing to PyPI"
fi

# Navigate to superbindings
cd "$(dirname "$0")"

# Check if maturin is installed
if ! command -v maturin &> /dev/null; then
    echo "📥 Installing maturin..."
    pip install maturin
fi

# Check for twine
if ! command -v twine &> /dev/null; then
    echo "📥 Installing twine..."
    pip install twine
fi

# Clean previous builds
echo "🧹 Cleaning previous builds..."
rm -rf target/wheels/*.whl

# Build wheels for current platform
echo "🔨 Building wheel..."
maturin build --release

# Find the wheel
WHEEL=$(ls target/wheels/*.whl | head -1)
echo "✅ Built: $WHEEL"

# Upload to PyPI
echo "📤 Uploading to PyPI..."
if [[ -n "$TEST_PYPI" ]]; then
    twine upload --repository testpypi "$WHEEL"
    echo "🎉 Published to TestPyPI!"
    echo "   Install with: pip install --index-url https://test.pypi.org/simple/ pysupertable"
else
    twine upload "$WHEEL"
    echo "🎉 Published to PyPI!"
    echo "   Install with: pip install pysupertable"
fi
