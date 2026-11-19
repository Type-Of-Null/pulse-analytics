#!/usr/bin/env bash
# Spin up a local Kubernetes cluster with the full Pulse stack (dev overlay) + KEDA + metrics-server.
set -euo pipefail
CLUSTER=${CLUSTER:-pulse}
KEDA_VERSION=${KEDA_VERSION:-2.17.2}
METRICS_SERVER_VERSION=${METRICS_SERVER_VERSION:-0.8.0}

kind get clusters | grep -qx "$CLUSTER" || kind create cluster --config deploy/kind/cluster.yaml --wait 120s
docker build -t pulse:local .
# Load single-platform archives: `kind load docker-image` fails on multi-arch images pulled into
# Docker's containerd image store ("content digest ... not found").
PLATFORM=$(docker version -f '{{.Server.Os}}/{{.Server.Arch}}')
ARCHIVE=$(mktemp)
trap 'rm -f "$ARCHIVE"' EXIT
for image in pulse:local apache/kafka:3.9.1 clickhouse/clickhouse-server:25.8-alpine postgres:17-alpine; do
  docker image inspect "$image" >/dev/null 2>&1 || docker pull -q --platform "$PLATFORM" "$image"
  docker save --platform "$PLATFORM" -o "$ARCHIVE" "$image"
  kind load image-archive "$ARCHIVE" --name "$CLUSTER"
done

kubectl apply -f "https://github.com/kubernetes-sigs/metrics-server/releases/download/v${METRICS_SERVER_VERSION}/components.yaml"
kubectl -n kube-system patch deploy metrics-server --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]' || true
kubectl apply --server-side -f "https://github.com/kedacore/keda/releases/download/v${KEDA_VERSION}/keda-${KEDA_VERSION}.yaml"
kubectl -n keda wait --for=condition=available deploy --all --timeout=300s

kubectl apply -k deploy/k8s/overlays/dev
kubectl -n pulse wait --for=condition=complete job/pulse-migrate --timeout=600s
kubectl -n pulse rollout restart deploy   # pods that crash-looped before migrations finished start clean
kubectl -n pulse wait --for=condition=available deploy --all --timeout=600s
kubectl -n pulse get pods
