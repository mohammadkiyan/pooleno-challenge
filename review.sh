#!/usr/bin/env bash
#
# The one command for a reviewer. It needs only Docker, and leaves the stack running.
#
#   ./review.sh               Build, run every check, prove the behaviour, keep the stack up
#   ./review.sh --skip-tests  The same without the typecheck, lint and test suite
#   ./review.sh down          Stop the stack and delete its data
#
# Move a published port that is already taken with HOST_PORT_API, HOST_PORT_PUBLISHER,
# HOST_PORT_MATCHING, HOST_PORT_EXTERNAL, HOST_PORT_POSTGRES, HOST_PORT_KAFKA or HOST_PORT_MOCK_API.
#
# Written for bash 3.2, which is what macOS ships.
set -uo pipefail

cd "$(dirname "$0")" || exit 1

LOG_DIR=.review-logs
SUMMARY=()

if [ -t 1 ]; then
  BOLD=$'\033[1m' GREEN=$'\033[32m' RED=$'\033[31m' DIM=$'\033[2m' RESET=$'\033[0m'
else
  BOLD='' GREEN='' RED='' DIM='' RESET=''
fi

# The checks image is behind a profile, so that `up` doesn't start it: it would exit, and fail --wait.
compose_with_checks() {
  docker compose --profile verify "$@"
}

start() {
  docker compose up -d --wait "$@"
}

# Runs a command in the checks image, on the stack's network.
in_verify() {
  compose_with_checks run --rm --no-deps -T verify "$@"
}

fail() {
  printf '\n%sFAILED:%s %s\n' "$RED$BOLD" "$RESET" "$1" >&2
  shift
  for line in "$@"; do
    printf '  %s\n' "$line" >&2
  done
  exit 1
}

duration() {
  if [ "$1" -ge 60 ]; then
    printf '%dm %02ds' $(($1 / 60)) $(($1 % 60))
  else
    printf '%ds' "$1"
  fi
}

# On failure: shows the end of this step's log, saves every service's logs, and leaves the stack up.
step_failed() {
  local title=$1 log=$2
  printf '%sfailed%s\n\n' "$RED" "$RESET" >&2
  tail -n 40 "$log" >&2
  docker compose ps -a >"$LOG_DIR/services.log" 2>&1
  docker compose logs --no-color >>"$LOG_DIR/services.log" 2>&1
  fail "$title" \
    "This step's full output: $log" \
    "Every service's logs:    $LOG_DIR/services.log" \
    "Remove the stack with:   ./review.sh down"
}

# step <title> <log name> <command...>: runs quietly, reporting the time it took.
step() {
  local title=$1 log="$LOG_DIR/$2.log" started=$SECONDS
  shift 2
  printf '%-50s ' "$title"
  if "$@" >"$log" 2>&1; then
    printf '%sok%s %s(%s)%s\n' "$GREEN" "$RESET" "$DIM" "$(duration $((SECONDS - started)))" "$RESET"
    SUMMARY+=("$title")
  else
    step_failed "$title" "$log"
  fi
}

# shown <title> <log name> <command...>: as step, but what it proves is printed as it happens.
shown() {
  local title=$1 log="$LOG_DIR/$2.log"
  shift 2
  printf '\n%s%s%s\n' "$BOLD" "$title" "$RESET"
  "$@" 2>&1 | tee "$log" | grep -E -v --line-buffered '^(Result: | *Container )'
  # Read straight after the pipeline, because any other command replaces PIPESTATUS.
  local status=${PIPESTATUS[0]}
  if [ "$status" -ne 0 ]; then
    step_failed "$title" "$log"
  fi
  SUMMARY+=("$title")
}

test_count() {
  sed -n 's/^ *Tests  *\([0-9]* passed\).*/\1/p' "$1" | tail -n 1
}

check_docker() {
  command -v docker >/dev/null 2>&1 ||
    fail 'Docker is not installed.' 'Install Docker with the Compose plugin, then run this again.'
  docker compose version >/dev/null 2>&1 ||
    fail 'Docker Compose v2 is not available.' 'This project uses the `docker compose` plugin.'
  docker info >/dev/null 2>&1 ||
    fail 'The Docker daemon is not reachable.' 'Start Docker, then run this again.'
}

# Published ports are checked after the reset, because this project's own stack may hold them.
check_ports() {
  local entry name port taken=()
  for entry in \
    "HOST_PORT_API:${HOST_PORT_API:-8080}" \
    "HOST_PORT_PUBLISHER:${HOST_PORT_PUBLISHER:-8081}" \
    "HOST_PORT_MATCHING:${HOST_PORT_MATCHING:-8082}" \
    "HOST_PORT_EXTERNAL:${HOST_PORT_EXTERNAL:-8083}" \
    "HOST_PORT_POSTGRES:${HOST_PORT_POSTGRES:-15432}" \
    "HOST_PORT_KAFKA:${HOST_PORT_KAFKA:-29092}" \
    "HOST_PORT_MOCK_API:${HOST_PORT_MOCK_API:-18080}"; do
    name=${entry%%:*}
    port=${entry##*:}
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      taken+=("Port $port is in use. Free it, or run: $name=<free port> ./review.sh")
    fi
  done
  if [ "${#taken[@]}" -gt 0 ]; then
    fail 'A port this project publishes is already in use.' "${taken[@]}"
  fi
}

reset_stack() {
  compose_with_checks down --volumes --remove-orphans
}

# Kills every process with no chance to clean up, then brings everything back.
kill_all_and_restart() {
  docker compose kill -s SIGKILL api publisher matching external mock-api postgres kafka && start
}

result_of() {
  sed -n 's/^Result: //p' "$1" | tail -n 1
}

compare_demo_runs() {
  local before after
  before=$(result_of "$LOG_DIR/demo.log")
  after=$(result_of "$LOG_DIR/demo-after-crash.log")
  if [ -z "$before" ] || [ "$before" != "$after" ]; then
    fail 'The results changed across the crash.' "Before: $before" "After:  $after"
  fi
  printf '  ok  The same orders, trades and effect as before the crash, and nothing new\n'
}

print_summary() {
  local item
  printf '\n%s%sALL CHECKS PASSED%s in %s\n\n' "$GREEN" "$BOLD" "$RESET" "$(duration "$SECONDS")"
  for item in "${SUMMARY[@]}"; do
    printf '  %sok%s  %s\n' "$GREEN" "$RESET" "$item"
  done
  if [ -n "${UNIT_TESTS:-}" ]; then
    printf '\n  Unit tests:                %s\n' "$UNIT_TESTS"
    printf '  Integration and e2e tests: %s\n' "$DATABASE_TESTS"
  fi
  cat <<EOF

The stack is still running, and these are worth a look:

  curl -s http://127.0.0.1:${HOST_PORT_API:-8080}/ops/backlog
  curl -s http://127.0.0.1:${HOST_PORT_MOCK_API:-18080}/__control/effects
  curl -s http://127.0.0.1:${HOST_PORT_MATCHING:-8082}/health/ready
  docker compose ps

Read next:       docs/architecture.md, then README.md for placing your own orders
Stop and delete: ./review.sh down
EOF
}

SKIP_TESTS=false
case "${1:-}" in
  '') ;;
  --skip-tests) SKIP_TESTS=true ;;
  down)
    check_docker
    reset_stack || fail 'The stack could not be removed.' 'The Compose output above says why.'
    rm -rf "$LOG_DIR"
    printf 'The stack and its data are gone.\n'
    exit 0
    ;;
  -h | --help)
    sed -n '3,10p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
  *) fail "Unknown argument: $1" 'Use ./review.sh, ./review.sh --skip-tests or ./review.sh down.' ;;
esac

check_docker
rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR"

printf '%sThis starts from nothing:%s it first deletes this Compose project'"'"'s containers and volumes.\n' "$BOLD" "$RESET"
printf 'Full output of each step is in %s/.\n\n' "$LOG_DIR"

step 'Remove any earlier run of this project' reset reset_stack
check_ports
step 'Build the images' build compose_with_checks build

if [ "$SKIP_TESTS" = false ]; then
  step 'Typecheck' typecheck in_verify npm run typecheck
  step 'Lint' lint in_verify npm run lint
  step 'Unit tests' test-unit in_verify npm run test:unit
  UNIT_TESTS=$(test_count "$LOG_DIR/test-unit.log")
  step 'Start PostgreSQL and Kafka for the tests' test-deps start postgres kafka
  step 'Integration and e2e tests (a few minutes)' test-database in_verify npm run test:integration
  DATABASE_TESTS=$(test_count "$LOG_DIR/test-database.log")
  step 'Remove what the tests left behind' reset-after-tests reset_stack
fi

step 'Start the whole stack' stack start

shown 'Walkthrough: acceptance, matching and notification' demo in_verify npm run --silent demo

shown 'Crash proof: before the crash' crash-prepare in_verify npx tsx scripts/crash-proof.ts prepare
step 'Kill the publisher with SIGKILL' kill-publisher docker compose kill -s SIGKILL publisher
shown 'Crash proof: an order saved with nothing to publish it' crash-accept \
  in_verify npx tsx scripts/crash-proof.ts accept
printf '\n'
step 'Kill every process with SIGKILL, then restart' crash-restart kill_all_and_restart
shown 'Crash proof: after the restart' crash-verify in_verify npx tsx scripts/crash-proof.ts verify
printf '\n'
step 'Run the walkthrough again, with the same keys' demo-after-crash in_verify npm run --silent demo
compare_demo_runs

print_summary
