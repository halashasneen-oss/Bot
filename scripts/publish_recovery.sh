#!/usr/bin/env bash
# Publish the local recovery tree to an existing repository using the user's own Git credentials.
# No force push, no secrets, no GitHub Actions. Run only when GitHub account access is in good standing.
set -euo pipefail
SOURCE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="https://github.com/halashasneen-oss/Bot.git"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
command -v git >/dev/null || { echo 'git is required'; exit 1; }
command -v python3 >/dev/null || { echo 'python3 is required'; exit 1; }
git clone "$TARGET" "$WORK/repo"
python3 - "$SOURCE" "$WORK/repo" <<'PY'
from pathlib import Path
import shutil, sys
src, dest = map(Path, sys.argv[1:])
for p in src.rglob('*'):
    rel = p.relative_to(src)
    if any(x in ('__pycache__', '.git', '.pytest_cache', '.venv', 'runs') for x in rel.parts) or p.suffix == '.pyc':
        continue
    q = dest / rel
    if p.is_dir(): q.mkdir(parents=True, exist_ok=True)
    elif p.is_file():
        q.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, q)
PY
cd "$WORK/repo"
git add -A
if git diff --cached --quiet; then echo 'Already up to date'; exit 0; fi
git commit -m 'Restore Polymarket PAPER bot with fixed T+225s strategy'
git push origin HEAD:main
echo 'Published to https://github.com/halashasneen-oss/Bot'
