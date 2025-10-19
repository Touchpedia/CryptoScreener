#!/usr/bin/env bash
set -e
try_pw() {
  PW="$1"
  if PGPASSWORD="$PW" psql -h 127.0.0.1 -U postgres -d csb_opt -At -c "select 'OK'"; then
    echo "PASS:$PW"
  else
    echo "FAIL:$PW"
  fi
}
try_pw 2715
try_pw postgres
