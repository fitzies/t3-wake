#!/bin/sh
# Remove the t3-wake service and CLI wrapper.
#
#   ./uninstall.sh          keep config, stored webhook and state (reinstalling resumes where it stopped)
#   ./uninstall.sh --purge  also delete ~/.config/t3-wake and ~/.local/state/t3-wake
set -eu

BIN_DIR=${BIN_DIR:-$HOME/.local/bin}
UNIT="$HOME/.config/systemd/user/t3-wake.service"

systemctl --user disable --now t3-wake.service 2>/dev/null || true
rm -f "$UNIT"
systemctl --user daemon-reload 2>/dev/null || true
if [ -f "$BIN_DIR/t3-wake" ] && grep -q "installed by .*install.sh" "$BIN_DIR/t3-wake"; then
  rm -f "$BIN_DIR/t3-wake"
fi

if [ "${1:-}" = "--purge" ]; then
  rm -rf "$HOME/.config/t3-wake" "$HOME/.local/state/t3-wake"
  echo "Removed t3-wake, its config, stored webhook and state."
else
  echo "Removed the t3-wake service and CLI. Config and state are kept in ~/.config/t3-wake and ~/.local/state/t3-wake."
fi
