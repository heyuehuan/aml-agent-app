#!/bin/bash
# Run as yuehuan (with sudo) on ist-server.thethreesigma.com
set -e

APP_USER=realtimeaml
APP_DIR=/opt/realtimeaml
REPO=https://github.com/heyuehuan/aml-agent-app.git

echo "==> Creating service account"
sudo useradd --system --shell /usr/sbin/nologin --home-dir $APP_DIR --create-home $APP_USER 2>/dev/null || echo "User already exists"

echo "==> Cloning repo"
sudo mkdir -p $APP_DIR
sudo git clone $REPO $APP_DIR/app || (cd $APP_DIR/app && sudo git pull)
sudo chown -R $APP_USER:$APP_USER $APP_DIR

echo "==> Installing uv"
sudo -u $APP_USER bash -c "curl -Ls https://astral.sh/uv/install.sh | sh"

echo "==> Installing Python dependencies"
sudo -u $APP_USER bash -c "cd $APP_DIR/app/backend && ~/.local/bin/uv sync"

echo "==> Creating data directory and placeholder DB"
sudo -u $APP_USER mkdir -p $APP_DIR/app/backend/aml_agent/data
# .env must be placed manually at: $APP_DIR/app/.env
# aml_transactions.db must be copied manually to: $APP_DIR/app/backend/aml_agent/data/

echo ""
echo "==> MANUAL STEPS REQUIRED before starting service:"
echo "    1. scp .env           -> $APP_DIR/app/.env"
echo "    2. scp aml_transactions.db -> $APP_DIR/app/backend/aml_agent/data/aml_transactions.db"
echo "    3. sudo chown $APP_USER:$APP_USER $APP_DIR/app/.env"
echo "    4. sudo chown $APP_USER:$APP_USER $APP_DIR/app/backend/aml_agent/data/aml_transactions.db"
echo "    Then run: sudo systemctl enable --now realtimeaml"
