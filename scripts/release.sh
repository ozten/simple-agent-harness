#!/usr/bin/env bash
#
# Cut a blacksmith release.
# Usage: ./scripts/release.sh 0.1.0
#

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <version>  (e.g. 0.1.0)"
    exit 1
fi

VERSION="$1"
TAG="v${VERSION}"

# Validate semver-ish format
if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Error: version must be semver (e.g. 1.2.3), got: $VERSION"
    exit 1
fi

# Check for clean working tree
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Error: working tree is dirty. Commit or stash changes first."
    exit 1
fi

# Check tag doesn't already exist
if git rev-parse "$TAG" &>/dev/null; then
    echo "Error: tag $TAG already exists"
    exit 1
fi

echo "Releasing blacksmith $TAG"

# Bump version in Cargo.toml files
sed -i.bak -E "0,/^version = \"[^\"]+\"/s//version = \"${VERSION}\"/" Cargo.toml
rm -f Cargo.toml.bak
sed -i.bak -E "0,/^version = \"[^\"]+\"/s//version = \"${VERSION}\"/" blacksmith-ui/Cargo.toml
rm -f blacksmith-ui/Cargo.toml.bak

# Update Cargo.lock
cargo check --quiet 2>/dev/null || true

# Commit + tag
git add Cargo.toml blacksmith-ui/Cargo.toml Cargo.lock
git commit -m "release: ${TAG}"
git tag "$TAG"

echo ""
echo "Created commit and tag $TAG"
echo ""
echo "Push to trigger the release build:"
echo "  git push origin main $TAG"
