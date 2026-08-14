#!/usr/bin/env bash
# Re-fetches the embedded *.local-ip.medicmobile.org fallback cert. The app
# refreshes its serving cert at runtime, so this only matters for the
# cold-start window; refresh occasionally before cutting a release.
set -euo pipefail
cd "$(dirname "$0")/.."
curl -sfL https://local-ip.medicmobile.org/fullchain -o certs/fullchain.pem
curl -sfL https://local-ip.medicmobile.org/key -o certs/key.pem
chmod 644 certs/fullchain.pem
chmod 600 certs/key.pem
openssl x509 -in certs/fullchain.pem -noout -subject -dates
echo "certs refreshed."
