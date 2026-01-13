#!/usr/bin/env sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../../.." && pwd)

# Default agent unless overridden via CLI.
AGENT="answer_4"
FORWARD_ARGS=""

escape_arg() {
  printf '%s' "$1" | sed 's/"/\\"/g'
}

while [ $# -gt 0 ]; do
  case "$1" in
    --agent)
      shift
      [ $# -gt 0 ] || { echo "error: --agent requires a value" >&2; exit 1; }
      AGENT="$1"
      shift
      ;;
    --agent=*)
      AGENT="${1#*=}"
      shift
      ;;
    *)
      FORWARD_ARGS="$FORWARD_ARGS \"$(escape_arg "$1")\""
      shift
      ;;
  esac
done

# Restore positional parameters for promptfoo (shellcheck disable via eval).
# shellcheck disable=SC2086
eval "set -- $FORWARD_ARGS"

cd "$PROJECT_ROOT"
PROMPTFOO_AGENT="$AGENT" npx promptfoo@latest eval -c evals/promptfoo_ext/promptfooconfig.yaml "$@"
