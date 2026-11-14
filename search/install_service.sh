#!/usr/bin/env bash
# Run on NODE 4:   sudo bash search/install_service.sh
# Installs + starts the search API as a systemd service. Fills in User= and
# WorkingDirectory= automatically, so it works whatever folder the repo was cloned into.
set -euo pipefail
[ "$(id -u)" -eq 0 ] || { echo "Run with sudo: sudo bash search/install_service.sh"; exit 1; }
HERE="$(cd "$(dirname "$0")" && pwd)"
RUN_AS="${SUDO_USER:-}"
[ -n "$RUN_AS" ] && [ "$RUN_AS" != "root" ] || { echo "Run it as your normal user with sudo, not as root"; exit 1; }

sed -e "s|^User=.*|User=$RUN_AS|" \
    -e "s|^WorkingDirectory=.*|WorkingDirectory=$HERE|" \
    "$HERE/search.service" > /etc/systemd/system/search.service
echo ">> installed service: User=$RUN_AS WorkingDirectory=$HERE"

systemctl daemon-reload
systemctl enable search
systemctl restart search
sleep 2
systemctl --no-pager --lines=5 status search || true
