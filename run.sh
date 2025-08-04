#!/bin/bash

set -e
set -o pipefail
set -u

trap 'echo "❌ Script failed on line $LINENO. Exiting."; read -p "Press Enter to close..."' ERR

echo "📁 Enter the full path of the drive or folder to mount:"
read -e -p "Path (e.g., /mnt/mydrive or C:/test_lfs): " USER_INPUT

# Normalize backslashes to slashes (Windows-style)
SHARED_DRIVE="${USER_INPUT//\\//}"

# If input is just a drive letter (e.g., C:), add a trailing slash
if [[ "$SHARED_DRIVE" =~ ^[A-Za-z]:$ ]]; then
  SHARED_DRIVE="$SHARED_DRIVE/"
fi

# Strip trailing slash only if it’s not a root drive
if [[ ! "$SHARED_DRIVE" =~ ^[A-Za-z]:/$ ]]; then
  SHARED_DRIVE="${SHARED_DRIVE%/}"
fi

echo "🔍 Checking if path exists: $SHARED_DRIVE"

# Now test for directory
if [ ! -d "$SHARED_DRIVE" ]; then
  echo "❌ Error: '$SHARED_DRIVE' does not exist on the host."
  read -p "Press Enter to close..."
  exit 1
fi

export HOST_SHARED_DRIVE="$SHARED_DRIVE"
echo "✅ HOST_SHARED_DRIVE set to: $HOST_SHARED_DRIVE"

docker compose up --build || {
  echo "❌ Docker Compose failed to start."
  read -p "Press Enter to close..."
  exit 1
}

