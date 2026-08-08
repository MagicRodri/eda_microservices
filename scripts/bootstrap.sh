#!/usr/bin/env bash
# Materialises the two service repositories under services/.
#
# They are separate repositories, so this monorepo only pins them. The script
# registers them as git submodules when possible and falls back to a plain
# clone when the working tree is not a git repository.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ORG="${ORG:-MagicRodri}"
BRANCH="${BRANCH:-main}"
cd "${ROOT}"

for repo in customer-service order-service; do
  target="services/${repo}"
  url="https://github.com/${ORG}/${repo}.git"

  if [ -f "${target}/go.mod" ]; then
    # Already there — update it rather than silently keeping whatever commit
    # was cloned first, which would quietly run stale service code.
    echo "Updating ${repo}"
    git -C "${target}" fetch --quiet origin "${BRANCH}" \
      && git -C "${target}" checkout --quiet "${BRANCH}" \
      && git -C "${target}" merge --quiet --ff-only "origin/${BRANCH}" \
      || echo "  could not fast-forward ${target}; leaving it alone"
    continue
  fi

  echo "Adding ${repo}"
  if git rev-parse --git-dir >/dev/null 2>&1; then
    git submodule add -b "${BRANCH}" "${url}" "${target}"
  else
    git clone --branch "${BRANCH}" "${url}" "${target}"
  fi
done

if git rev-parse --git-dir >/dev/null 2>&1; then
  git submodule update --init --recursive
fi

echo
echo "Services ready:"
ls -1 services
