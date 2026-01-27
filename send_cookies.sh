#!/bin/bash
# send_cookies.sh - Envia cookies de X.com al servidor
#
# Uso:
#   1. Logueate en X.com en tu browser
#   2. Cookie-Editor -> Export -> JSON -> Guardar como ~/Downloads/x_cookies.json
#   3. Correr: ./send_cookies.sh

set -e

# Configuracion
SERVER_USER="jmt"
SERVER_NAME="servidor-outlier"
SERVER_PATH="~/dev/python/twitter_scraper/cookies.json"
DEFAULT_COOKIE_FILE="$HOME/Downloads/x_cookies.json"

# Obtener IP dinamica desde GCP
echo "Obteniendo IP del servidor desde GCP..."
SERVER_HOST=$(gcloud compute instances list --filter="name=$SERVER_NAME" --format="value(networkInterfaces[0].accessConfigs[0].natIP)" 2>/dev/null)

if [ -z "$SERVER_HOST" ]; then
    echo "Error: No pude obtener la IP del servidor $SERVER_NAME"
    echo "Verifica que gcloud este configurado y la VM este corriendo"
    exit 1
fi

echo "Servidor encontrado: $SERVER_HOST"

COOKIE_FILE="${1:-$DEFAULT_COOKIE_FILE}"

if [ ! -f "$COOKIE_FILE" ]; then
    echo "No encontre archivo de cookies en: $COOKIE_FILE"
    echo ""
    echo "Instrucciones:"
    echo "  1. Abri X.com en tu browser y logueate"
    echo "  2. Click en Cookie-Editor -> Export -> JSON"
    echo "  3. Guarda como: $DEFAULT_COOKIE_FILE"
    echo "  4. Corre: ./send_cookies.sh"
    exit 1
fi

echo "Procesando cookies desde: $COOKIE_FILE"

TEMP_FILE=$(mktemp)

python3 - "$COOKIE_FILE" > "$TEMP_FILE" << 'EOF'
import json, sys
try:
    with open(sys.argv[1], "r") as f:
        data = json.load(f)
    if isinstance(data, list):
        cookies_dict = {}
        for c in data:
            domain = c.get("domain", "")
            if ".x.com" in domain or ".twitter.com" in domain or "x.com" in domain:
                cookies_dict[c.get("name", "")] = c.get("value", "")
        cookies_dict["user-agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        if "auth_token" not in cookies_dict and "ct0" not in cookies_dict:
            print("Error: No se encontraron cookies de auth", file=sys.stderr)
            sys.exit(1)
        print(json.dumps(cookies_dict, indent=2))
    else:
        print(json.dumps(data, indent=2))
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)
EOF

if [ $? -ne 0 ]; then
    echo "Error procesando cookies"
    rm -f "$TEMP_FILE"
    exit 1
fi

echo "Enviando al servidor $SERVER_HOST..."

scp -i ~/.ssh/gcp_key -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "$TEMP_FILE" "${SERVER_USER}@${SERVER_HOST}:${SERVER_PATH}"

if [ $? -eq 0 ]; then
    echo "Cookies enviadas!"
    rm -f "$TEMP_FILE" "$COOKIE_FILE"
else
    echo "Error enviando cookies"
    rm -f "$TEMP_FILE"
    exit 1
fi
