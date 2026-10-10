#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Change to the script directory to ensure relative paths work correctly
cd "$SCRIPT_DIR"

BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
BUILD_ID=$(date -u +%Y%m%d)
GIT_SHA=$(git rev-parse HEAD)

python - <<EOF
from pathlib import Path
import re

path = Path("pyproject.toml")
text = path.read_text()

section = """[tool.mfi_ddb.build]
date = "${BUILD_DATE}"
id = "${BUILD_ID}"
commit = "${GIT_SHA}"
"""

pattern = r'(?ms)^\[tool\.mfi_ddb\.build\]\s*.*?(?=^\[|\Z)'

if re.search(pattern, text):
    text = re.sub(pattern, section + "\n", text)
else:
    text = text.rstrip() + "\n\n" + section + "\n"

path.write_text(text)
EOF

echo "Build metadata:"
grep -A3 '^\[tool.mfi_ddb.build\]' pyproject.toml
