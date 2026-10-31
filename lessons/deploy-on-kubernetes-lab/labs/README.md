# Operate Collector topologies in a disposable cluster runnable files

Docker, k3d, kubectl and an explicitly selected disposable kubeconfig/context. Follow the topology setup instructions and pinned images. Never use a shared or production context.

Extract this complete archive into the shared course workspace, preserving paths.
Learning instructions and expected observations are provided in the enrolled
course app. Generated evidence and credentials do not belong in source control.

The included `lessons/deployment-topologies/labs/setup.sh` creates the pinned
disposable cluster using the explicitly selected local Docker context. It
requires the GitHub CLI (`gh`) to retrieve the pinned controller manifests.

## Runner interfaces

- `lessons/deployment-topologies/labs/run.py` (from `lessons/deployment-topologies`): run with `--help` for its required arguments.
- `lessons/node-pod-logs/labs/run.py` (from `lessons/node-pod-logs`): run with `--help` for its required arguments.
- `lessons/cluster-telemetry/labs/run.py` (from `lessons/cluster-telemetry`): run with `--help` for its required arguments.
- `lessons/opentelemetry-operator/labs/run.py` (from `lessons/opentelemetry-operator`): run with `--help` for its required arguments.

Native runners use the pinned Collector; Kubernetes runners require an explicit
`--kubeconfig` and `--context`. Kafka requires its pinned image and an explicit
Docker context. Run only against lab-owned local resources.
