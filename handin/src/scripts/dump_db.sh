set -euo pipefail
set -a; [ -f .env ] && . .env; set +a
grep -qxF 'backups/*' .gitignore      || echo 'backups/*' >> .gitignore
grep -qxF '!backups/.gitkeep' .gitignore || echo '!backups/.gitkeep' >> .gitignore
mkdir -p backups && touch backups/.gitkeep
TS="$(date +%F_%H%M%S)"
PORT="${PGPORT:-5432}"
DB="${PGDATABASE:-nhl_beyond}"

TMP="backups/.tmp_${DB}_${TS}.dump"
OUT="backups/${DB}_${TS}.dump"
LOG="backups/pg_dump_${TS}.log"
: "${PGHOST:?set PGHOST}"; : "${PGUSER:?set PGUSER}"; : "${PGPASSWORD:?set PGPASSWORD}"

# make the dump to a temp file (atomic), log stderr
PGPASSWORD="$PGPASSWORD" \
pg_dump -h "$PGHOST" -p "$PORT" -U "$PGUSER" -d "$DB" \
  -Fc --compress=9 --no-owner --no-privileges --verbose \
  -f "$TMP" 2> "$LOG"

mv "$TMP" "$OUT"

# 2) quick sanity check (lists TOC items)
pg_restore -l "$OUT" | head

# 3) optional integrity hash
shasum -a 256 "$OUT" > "${OUT}.sha256"

# 4) upload to S3 (encrypt at rest) — only if bucket is set
if [[ -n "${S3_BUCKET_NAME:-}" ]]; then
  aws s3 cp "$OUT" "s3://$S3_BUCKET_NAME/backups/" --sse AES256
  aws s3 cp "${OUT}.sha256" "s3://$S3_BUCKET_NAME/backups/" --sse AES256
fi

echo "✅ Wrote: $OUT"
echo "📝 Log : $LOG"
