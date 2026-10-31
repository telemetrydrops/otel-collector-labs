#!/usr/bin/env bash
set -euo pipefail
cluster_name=${1:?Usage: setup.sh dropzone-collector-NAME /absolute/temporary/kubeconfig [docker-context]}
kubeconfig_path=${2:?Provide a separate absolute kubeconfig path}
docker_context=${3:-colima}
[[ "$cluster_name" == dropzone-collector-* && ${#cluster_name} -le 32 && "$cluster_name" =~ ^[a-z0-9-]+$ ]] || { echo 'Use a new dropzone-collector-* name, at most 32 characters'; exit 1; }
[[ "$kubeconfig_path" == /* && ! -e "$kubeconfig_path" ]] || { echo 'Kubeconfig must be a new absolute file path'; exit 1; }
export DOCKER_HOST
DOCKER_HOST=$(docker context inspect "$docker_context" --format '{{.Endpoints.docker.Host}}')
[[ "$DOCKER_HOST" == unix://* ]] || { echo 'This tested setup requires a local Unix Docker socket'; exit 1; }
if k3d cluster list --output json | python3 -c 'import json,sys; name=sys.argv[1]; sys.exit(0 if any(c["name"]==name for c in json.load(sys.stdin)) else 1)' "$cluster_name"; then
  echo 'Refusing to reuse an existing cluster'; exit 1
fi
work_dir=$(mktemp -d)
created=false
complete=false
cleanup() {
  rm -rf "$work_dir"
  if [[ "$created" == true && "$complete" == false ]]; then
    k3d cluster delete "$cluster_name"
    rm -f "$kubeconfig_path"
  fi
}
trap cleanup EXIT
api_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
k3d cluster create "$cluster_name" --image rancher/k3s:v1.35.5-k3s1 --servers 1 --agents 1 --servers-memory 2g --agents-memory 1g --no-lb --api-port "127.0.0.1:$api_port" --k3s-arg '--disable=traefik@server:0' --kubeconfig-update-default=false --kubeconfig-switch-context=false --timeout 120s
created=true
umask 077
k3d kubeconfig get "$cluster_name" > "$kubeconfig_path"
context="k3d-$cluster_name"
kube=(kubectl --kubeconfig "$kubeconfig_path" --context "$context")
"${kube[@]}" wait nodes --all --for=condition=Ready --timeout=90s
gh release download v1.19.1 --repo cert-manager/cert-manager --pattern cert-manager.yaml --dir "$work_dir"
"${kube[@]}" apply -f "$work_dir/cert-manager.yaml"
"${kube[@]}" -n cert-manager rollout status deployment/cert-manager-webhook --timeout=90s
gh release download v0.161.0 --repo open-telemetry/opentelemetry-operator --pattern opentelemetry-operator.yaml --dir "$work_dir"
"${kube[@]}" apply --server-side -f "$work_dir/opentelemetry-operator.yaml"
"${kube[@]}" -n opentelemetry-operator-system rollout status deployment/opentelemetry-operator-controller-manager --timeout=90s
complete=true
printf 'Dedicated context: %s\nKubeconfig: %s\nCleanup only this cluster: DOCKER_HOST=%q k3d cluster delete %q\n' "$context" "$kubeconfig_path" "$DOCKER_HOST" "$cluster_name"
