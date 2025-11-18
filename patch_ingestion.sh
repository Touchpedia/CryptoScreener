#!/bin/sh
set -e

echo "[INFO] Searching FastAPI apps in /app ..."
files=$(grep -RIl 'FastAPI(' /app 2>/dev/null || true)
if [ -z "$files" ]; then
  echo "[ERR] Koi FastAPI app file nahi mili."
  exit 2
fi

echo "$files" | sed -n '1,200p'

for f in $files; do
  echo "[INFO] Patching $f"
  cp "$f" "$f.bak" 2>/dev/null || true

  # galat injected lines saaf
  sed -i '/api\.ingestion_fix/d' "$f"
  sed -i '/ingestion_fix_router/d' "$f"
  sed -i 's/^[\\]nfrom /from /' "$f"

  # import add (agar missing ho)
  if ! grep -q 'from api\.ingestion_fix import router as ingestion_fix_router' "$f"; then
    printf '\nfrom api.ingestion_fix import router as ingestion_fix_router\n' >> "$f"
  fi

  # include_router add (agar missing ho)
  if ! grep -q 'app\.include_router(ingestion_fix_router)' "$f"; then
    printf 'app.include_router(ingestion_fix_router)\n' >> "$f"
  fi
done

# router file exist?
if [ ! -f /app/api/ingestion_fix.py ]; then
  echo "[ERR] /app/api/ingestion_fix.py missing hai."
  exit 3
fi

echo "[OK] Patch done."