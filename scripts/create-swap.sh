#!/bin/sh
set -eu

SWAP_FILE=${SWAP_FILE:-/swapfile}
SWAP_SIZE_MB=${SWAP_SIZE_MB:-1024}

if [ "$(id -u)" -ne 0 ]; then
  echo "Запустите скрипт от root." >&2
  exit 1
fi
if swapon --show=NAME --noheadings | grep -Fxq "$SWAP_FILE"; then
  echo "Swap уже включён: $SWAP_FILE"
  exit 0
fi
if [ ! -f "$SWAP_FILE" ]; then
  fallocate -l "${SWAP_SIZE_MB}M" "$SWAP_FILE" || dd if=/dev/zero of="$SWAP_FILE" bs=1M count="$SWAP_SIZE_MB"
  chmod 600 "$SWAP_FILE"
  mkswap "$SWAP_FILE"
fi
swapon "$SWAP_FILE"
grep -Fq "$SWAP_FILE none swap sw 0 0" /etc/fstab || printf '%s none swap sw 0 0\n' "$SWAP_FILE" >> /etc/fstab
echo "Swap включён: $SWAP_FILE (${SWAP_SIZE_MB} MB)"
