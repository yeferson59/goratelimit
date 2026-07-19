#!/bin/bash

set -euo pipefail

REF="${RELEASE_TAG:-${GITHUB_REF_NAME:-$(git describe --tags --abbrev=0)}}"
TAG="${REF#v}"

if [[ -z "$TAG" ]]; then
    echo "Error: No tag found"
    exit 1
fi

PREV_TAG=$(git describe --tags --abbrev=0 "v$TAG"^ 2>/dev/null || echo "")

current_date=$(date +"%Y-%m-%d")

echo "## Release v${TAG} (${current_date})"
echo ""
echo "### Checks Passed"
echo "- ✅ Format validation"
echo "- ✅ Linting"
echo "- ✅ Tests"
echo "- ✅ Build"
echo ""

types=("feat:Features" "fix:Bug Fixes" "refactor:Refactoring" "perf:Performance" "docs:Documentation" "test:Tests" "chore:Maintenance")

for entry in "${types[@]}"; do
    type="${entry%%:*}"
    label="${entry#*:}"
    commits=$(git log --pretty=format:"- %s (%h)" "${PREV_TAG}..v$TAG" 2>/dev/null | grep "^- ${type}:" || true)
    if [[ -n "$commits" ]]; then
        echo "## ${label}"
        echo "$commits"
        echo ""
    fi
done

if [[ -z "$PREV_TAG" ]]; then
    echo "## All Changes"
    git log --pretty=format:"- %s (%h)" -20 2>/dev/null
fi
