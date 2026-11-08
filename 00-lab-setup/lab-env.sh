# Lab shell helpers for the `local` environment. Source it in every new terminal:
#
#   source ~/labs/confluent-crash-course/00-lab-setup/lab-env.sh     # bash or zsh
#   lab_check                                                        # optional sanity check
#
# Defines LAB, DCA_ID, DCB_ID and the functions a, b, sra, srb (run a tool inside a container).

if [ -n "${BASH_VERSION:-}" ]; then
  _lab_self="${BASH_SOURCE[0]}"
elif [ -n "${ZSH_VERSION:-}" ]; then
  eval '_lab_self="${(%):-%x}"'      # eval so bash never parses zsh syntax
else
  _lab_self="${LAB:-.}/lab-env.sh"
fi
LAB="$(cd "$(dirname "$_lab_self")" && pwd)"
export LAB
unset _lab_self

DCA_ID=Q0PvpgzYSwirapvmRY0NFg          # fixed CLUSTER_IDs from docker-compose.yml
DCB_ID=UmwHWmvUR6KH9yIQ3xbmhg

a()   { docker exec -i dc-a "$@"; }    # Kafka CLI tools inside dc-a
b()   { docker exec -i dc-b "$@"; }    # ... inside dc-b
sra() { docker exec -i sr-a "$@"; }    # Schema Registry tools
srb() { docker exec -i sr-b "$@"; }

# Checks the things that most often go wrong before a lab: wrong Docker host, containers down,
# $LAB/work not shared with the containers, REST ports not reachable from this shell.
lab_check() {
  local ok=1 c f
  echo "docker context: $(docker context show 2>/dev/null)${DOCKER_HOST:+ (DOCKER_HOST=$DOCKER_HOST)}"
  for c in dc-a dc-b sr-a sr-b; do
    if [ "$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null)" = true ]; then
      echo "ok    $c running"
    else
      echo "FAIL  $c not running on this Docker host (cd \$LAB && docker compose up -d)"; ok=0
    fi
  done
  f=".lab-check-$$"
  : > "$LAB/work/$f"
  if a test -f "/work/$f" 2>/dev/null; then
    echo "ok    \$LAB/work is visible as /work in the containers"
  else
    echo "FAIL  \$LAB/work is NOT visible as /work (remote Docker host? see README, 'Remote Docker host')"; ok=0
  fi
  rm -f "$LAB/work/$f"
  for c in 8090 8091; do
    if curl -sf -m 3 "localhost:$c/kafka/v3/clusters" >/dev/null; then
      echo "ok    REST v3 on localhost:$c"
    else
      echo "FAIL  no REST v3 on localhost:$c from this shell"; ok=0
    fi
  done
  if [ "$(docker inspect -f '{{.State.Running}}' connect 2>/dev/null)" = true ]; then
    echo "ok    connect running (only needed for module 01 part C)"
  else
    echo "info  connect not running (only needed for module 01 part C: docker compose --profile mm2 up -d)"
  fi
  [ $ok = 1 ] && echo "All good." || return 1
}
