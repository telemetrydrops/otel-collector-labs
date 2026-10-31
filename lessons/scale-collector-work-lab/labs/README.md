# Scale routes while preserving state ownership runnable files

Python 3.11+ and Collector Contrib 0.163.1. Target allocation additionally requires the explicit disposable Kubernetes kubeconfig/context described in that step.

Extract this complete archive into the shared course workspace, preserving paths.
Learning instructions and expected observations are provided in the enrolled
course app. Generated evidence and credentials do not belong in source control.

The included `lessons/deployment-topologies/labs/setup.sh` creates the pinned
disposable cluster using the explicitly selected local Docker context. It
requires the GitHub CLI (`gh`) to retrieve the pinned controller manifests.

## Runner interfaces

- `lessons/scale-bottlenecks/labs/run.py` (from `lessons/scale-bottlenecks`): run with `--help` for its required arguments.
- `lessons/client-load-balancing/labs/run.py` (from `lessons/client-load-balancing`): run with `--help` for its required arguments.
- `lessons/prometheus-target-allocation/labs/run.py` (from `lessons/prometheus-target-allocation`): run with `--help` for its required arguments.
- `lessons/scale-tail-sampling/labs/run.py` (from `lessons/scale-tail-sampling`): run with `--help` for its required arguments.

Native runners use the pinned Collector; Kubernetes runners require an explicit
`--kubeconfig` and `--context`. Kafka requires its pinned image and an explicit
Docker context. Run only against lab-owned local resources.
