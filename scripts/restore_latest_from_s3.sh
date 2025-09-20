#!/usr/bin/env bash
set -euo pipefail

# Inputs (env vars). Sensible defaults:
BUCKET="${S3_BUCKET_NAME:-${BUCKET:-}}"
PROFILE="${AWS_PROFILE:-}"
REGION="${AWS_REGION:-}"
TARGET_DB="${TARGET_DB:-${PGDATABASE:-nhl_beyond}}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"

# Preflight
[[ -n "$BUCKET" ]] || { echo "Set S3_BUCKET_NAME (or BUCKET)"; exit 2; }
command -v aws >/dev/null || { echo "aws CLI not found"; exit 2; }
command -v pg_restore >/dev/null || { echo "pg_restore not found"; exit 2; }
[[ -n "${PGPASSWORD:-}" ]] || { echo "Set PGPASSWORD"; exit 2; }

AWS_ARGS=()
[[ -n "$PROFILE" ]] && AWS_ARGS+=(--profile "$PROFILE")
[[ -n "$REGION"  ]] && AWS_ARGS+=(--region "$REGION")

echo "🔎 Finding latest .dump under s3://$BUCKET/backups/ ..."
key=$(aws s3api list-objects-v2 --bucket "$BUCKET" --prefix "backups/" \
  --query 'reverse(sort_by(Contents[?ends_with(Key, `.dump`) == `true`], &LastModified))[0].Key' \
  --output text "${AWS_ARGS[@]}")

[[ -n "$key" && "$key" != "None" ]] || { echo "No .dump found."; exit 3; }
file="$(basename "$key")"

echo "⬇️  Downloading $key"
aws s3 cp "s3://$BUCKET/$key" "." "${AWS_ARGS[@]}"
aws s3 cp "s3://$BUCKET/${key}.sha256" "." "${AWS_ARGS[@]}"

echo "🔐 Verifying checksum..."
expected=$(awk '{print $1}' "${file}.sha256")
actual=$(shasum -a 256 "$file" | awk '{print $1}')
[[ "$expected" == "$actual" ]] || { echo "Checksum mismatch"; exit 4; }
echo "✅ Checksum OK"

if [[ "${RESET_DB:-0}" != "0" ]]; then
  dropdb  -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" "$TARGET_DB" || true
  createdb -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" "$TARGET_DB"
fi

echo "🛠️  Restoring into $TARGET_DB on $PGHOST:$PGPORT as $PGUSER ..."
pg_restore -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$TARGET_DB" \
  --no-owner --no-privileges --clean --if-exists "$file"

echo "🎉 Done. Remove local files if you like:"
echo "    rm -f -- '$file' '${file}.sha256'"
