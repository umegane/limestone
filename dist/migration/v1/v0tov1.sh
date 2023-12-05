#! /bin/sh

# converting limestone v0 dir to v1 dir

USAGE="\
usage: $0 OLD_DB_DIR DEST_DB_DIR
	convert data in OLD_DB_DIR and store into DEST_DB_DIR.
	DEST_DB_DIR must not contain any data.
   or: $0 -w DB_DIR
	convert data in DB_DIR inplace."

set -e

error () { echo "$*" >&2; exit 1; }

check_from_dir () {
  local dbdir
  dbdir="$1"
  [ -d "$dbdir" ] || error "error: directory '$dbdir' not found."
  [ -r "$dbdir/epoch" ] \
    || error "error: directory '$dbdir' does not look like DB_DIR."
  [ ! -e "$dbdir/limestone-manifest.json" ] \
    || error "error: directory '$dbdir' does not look like v0 DB_DIR. already converted?"
  expr '(' `stat --format=%s "$dbdir/epoch"` % 9 ')' = 0 > /dev/null \
    || error "error: epoch file '$dbdir/epoch' is broken."
  [ -n "`ls -A1q "$dbdir"/pwal_* 2>/dev/null | head -1`" ] \
    || error "error: directory '$dbdir' does not look like DB_DIR. no wal files found. no need to migrate?"
}

check_to_dir () {
  [ -w "$1" ] || error "error: directory '$1' is not writable."
}

check_dir_empty () {
  [ -z "`ls -A1q "$1" | head -1`" ] || error "error: '$1' is not empty."
}

create_or_append_files () {
  printf '\004\377\377\377\377\377\377\000\000' >> "$1/epoch"
  cat > "$1/limestone-manifest.json" <<'EOD'
{
    "format_version": "1.0",
    "persistent_format_version": 1
}
EOD
}


[ -n "$2" ] || error "$USAGE"
[ -z "$3" ] || error "$USAGE"

echo checking...

if [ "x$1" = "x-w" ]; then
  # overwrite mode
  check_from_dir "$2"
  check_to_dir "$2"
  [ -w "$2/epoch" ] || error "error: file '$2/epoch' is not writable."
  echo "updating DB_DIR ($2) format from v0 to v1 ..."
  create_or_append_files "$2"
  echo "done, updated DB_DIR ($2) format to v1."
else
  check_from_dir "$1"
  if [ -d "$2" ]; then
    check_to_dir "$2"
    check_dir_empty "$2"
  elif [ -e "$2" ]; then
    error "error: DEST_DB_DIR '$2' is not directory"
  else
    mkdir -p "$2"
  fi
  echo "creating new v1 DB_DIR ($2) from old v0 DB_DIR ($1) ..."
  cp -p "$1"/pwal* "$2"/
  chmod u+w "$2"/pwal_0??? 2>/dev/null || true
  create_or_append_files "$2"
  echo "done, created new v1 DB_DIR ($2)."
fi
