#!/bin/sh
# Install t3-wake for the current user. The CLI and the systemd user service both run
# t3_wake.py from this checkout, so `git pull` + `systemctl --user restart t3-wake` upgrades it.
#
#   ./install.sh             install and start (a fresh install starts in dry-run mode)
#   ./install.sh --no-start  install without starting the service
set -eu

REPO=$(cd "$(dirname "$0")" && pwd)
SCRIPT="$REPO/t3_wake.py"
PYTHON=${PYTHON:-$(command -v python3 || true)}
BIN_DIR=${BIN_DIR:-$HOME/.local/bin}
UNIT_DIR="$HOME/.config/systemd/user"
UNIT="$UNIT_DIR/t3-wake.service"
START=1
[ "${1:-}" = "--no-start" ] && START=0

die() { echo "install.sh: $*" >&2; exit 1; }

[ -n "$PYTHON" ] || die "python3 not found"
"$PYTHON" -c 'import sys; sys.exit(sys.version_info < (3, 9))' || die "Python 3.9 or newer is required"
command -v systemctl >/dev/null || die "systemd is required (systemctl not found)"
case "$REPO$PYTHON" in
  *[[:space:]%]*) die "paths with spaces or % are not supported: $REPO" ;;
esac
command -v systemd-creds >/dev/null || echo "note: systemd-creds not found; set-webhook will use a 0600 file instead"

mkdir -p "$BIN_DIR" "$UNIT_DIR"
mkdir -p -m 700 "$HOME/.config/t3-wake" "$HOME/.local/state/t3-wake"

cat > "$BIN_DIR/t3-wake" <<WRAPPER
#!/bin/sh
# t3-wake CLI wrapper (installed by $REPO/install.sh)
exec "$PYTHON" -I "$SCRIPT" "\$@"
WRAPPER
chmod 755 "$BIN_DIR/t3-wake"

sed -e "s|@PYTHON@|$PYTHON|g" -e "s|@SCRIPT@|$SCRIPT|g" -e "s|@REPO@|$REPO|g" \
  "$REPO/systemd/t3-wake.service.in" > "$UNIT"
systemctl --user daemon-reload

if [ "$START" = 1 ]; then
  systemctl --user enable t3-wake.service
  systemctl --user restart t3-wake.service  # starts it, or picks up new code if it was running
else
  systemctl --user enable t3-wake.service
fi

echo "Installed: $BIN_DIR/t3-wake and $UNIT (running $SCRIPT)"
case ":$PATH:" in *":$BIN_DIR:"*) ;; *) echo "note: $BIN_DIR is not on your PATH" ;; esac
if [ "$(loginctl show-user "$(id -un)" -p Linger --value 2>/dev/null || true)" != "yes" ]; then
  echo "note: linger is off, so the service stops when you log out. To keep it running:"
  echo "      loginctl enable-linger $(id -un)"
fi
echo "Next: t3-wake set-webhook, t3-wake test, t3-wake live, t3-wake status"
