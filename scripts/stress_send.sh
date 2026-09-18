#!/usr/bin/env bash
# لفاف نازک روی stress_send.py — از هر جا قابل اجراست.
# مثال امن:
#   BASE_URL=http://localhost:8080 USER_ID=1 N=500 CONC=40 ./stress_send.sh mix
#   TOPUP=100000 N=2000 CONC=50 ./stress_send.sh express
#
# خطرناک (عمدی): N خیلی بزرگ فقط با --force
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$DIR/stress_send.py" "$@"
