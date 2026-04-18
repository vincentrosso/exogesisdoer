#!/usr/bin/env bash
# deploy/setup.sh — one-command Hetzner Ubuntu 22.04 setup for doer.ndex.us
#
# Usage (run as root or with sudo):
#   bash setup.sh
#
# What it does:
#   1. Installs system deps (Python 3.11, nginx, certbot, git, ufw)
#   2. Creates a 'doer' service user
#   3. Clones / pulls the repo to /opt/exogesisdoer
#   4. Creates .venv and installs Python deps
#   5. Generates a random web secret
#   6. Installs the systemd service
#   7. Configures nginx + gets a Let's Encrypt certificate
#   8. Opens firewall ports 80 and 443
#   9. Starts everything
#
# Re-running this script is safe (idempotent).

set -euo pipefail

DOMAIN="doer.ndex.us"
REPO="https://github.com/vincentrosso/exogesisdoer.git"
INSTALL_DIR="/opt/exogesisdoer"
SERVICE_USER="doer"
PYTHON="python3"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[setup]${NC} $*"; }
warn()    { echo -e "${YELLOW}[warn]${NC}  $*"; }
die()     { echo -e "${RED}[error]${NC} $*" >&2; exit 1; }

[[ $EUID -eq 0 ]] || die "Run as root: sudo bash deploy/setup.sh"

# ── 1. system deps ────────────────────────────────────────────────────────────
info "Updating apt and installing system packages…"
apt-get update -q
apt-get install -y -q \
    python3 python3-venv python3-dev \
    nginx certbot python3-certbot-nginx \
    git curl ufw build-essential

# ── 2. service user ───────────────────────────────────────────────────────────
if ! id "$SERVICE_USER" &>/dev/null; then
    info "Creating service user '$SERVICE_USER'…"
    useradd --system --home "$INSTALL_DIR" --shell /bin/false "$SERVICE_USER"
fi

# ── 3. clone / update repo ────────────────────────────────────────────────────
if [[ -d "$INSTALL_DIR/.git" ]]; then
    info "Pulling latest code into $INSTALL_DIR…"
    sudo -u "$SERVICE_USER" git -C "$INSTALL_DIR" pull --ff-only
else
    info "Cloning repo to $INSTALL_DIR…"
    git clone "$REPO" "$INSTALL_DIR"
    chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR"
fi

# ── 4. python venv ────────────────────────────────────────────────────────────
VENV="$INSTALL_DIR/.venv"
if [[ ! -d "$VENV" ]]; then
    info "Creating Python venv…"
    sudo -u "$SERVICE_USER" $PYTHON -m venv "$VENV"
fi
info "Installing Python dependencies…"
sudo -u "$SERVICE_USER" "$VENV/bin/pip" install -q --upgrade pip
sudo -u "$SERVICE_USER" "$VENV/bin/pip" install -q -r "$INSTALL_DIR/requirements.txt"

# ── 5. web secret ─────────────────────────────────────────────────────────────
CFG="$INSTALL_DIR/config.yaml"
if grep -q "change-me-before-deploying" "$CFG"; then
    SECRET=$(openssl rand -hex 32)
    sed -i "s/change-me-before-deploying/$SECRET/" "$CFG"
    info "Generated web secret."
    info "  → Save this key to log in: ${YELLOW}$SECRET${NC}"
    echo "$SECRET" > /root/doer_web_secret.txt
    chmod 600 /root/doer_web_secret.txt
    info "  → Also saved to /root/doer_web_secret.txt"
else
    info "Web secret already set — skipping."
fi

# ── 6. create required dirs ───────────────────────────────────────────────────
for d in output cache; do
    DIR="$INSTALL_DIR/$d"
    mkdir -p "$DIR"
    chown "$SERVICE_USER:$SERVICE_USER" "$DIR"
done

# ── 7. systemd service ────────────────────────────────────────────────────────
info "Installing systemd service…"
cp "$INSTALL_DIR/deploy/exogesisdoer.service" /etc/systemd/system/
systemctl daemon-reload
systemctl enable exogesisdoer
systemctl restart exogesisdoer
info "Service status: $(systemctl is-active exogesisdoer)"

# ── 8. nginx ──────────────────────────────────────────────────────────────────
NGINX_CONF="/etc/nginx/sites-available/$DOMAIN"
info "Configuring nginx for $DOMAIN…"
cp "$INSTALL_DIR/deploy/nginx.conf" "$NGINX_CONF"
ln -sf "$NGINX_CONF" "/etc/nginx/sites-enabled/$DOMAIN"
# Remove default site if it exists
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl reload nginx

# ── 9. Let's Encrypt ──────────────────────────────────────────────────────────
CERT_PATH="/etc/letsencrypt/live/$DOMAIN/fullchain.pem"
if [[ -f "$CERT_PATH" ]]; then
    info "SSL certificate already exists — skipping certbot."
else
    info "Obtaining Let's Encrypt certificate for $DOMAIN…"
    # Temporarily serve HTTP for ACME challenge using the http-only nginx block
    # The nginx.conf already has a redirect, so we use standalone challenge
    certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos \
        -m "vincentrosso@gmail.com" --redirect
fi

# ── 10. firewall ──────────────────────────────────────────────────────────────
info "Configuring ufw firewall…"
ufw allow OpenSSH
ufw allow 'Nginx Full'
ufw --force enable
ufw status

# ── done ──────────────────────────────────────────────────────────────────────
systemctl reload nginx
info ""
info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
info "  doer.ndex.us is live!"
info ""
info "  URL:      https://$DOMAIN"
info "  Service:  systemctl status exogesisdoer"
info "  Logs:     journalctl -u exogesisdoer -f"
info "  Key:      cat /root/doer_web_secret.txt"
info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
