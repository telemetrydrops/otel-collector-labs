# Build and diagnose your first pipeline runnable files

Python 3.11+ and the pinned native Collector Contrib 0.163.1. All receivers in this module use loopback.

Extract this complete archive into the shared course workspace, preserving paths.
Learning instructions and expected observations are provided in the enrolled
course app. Generated evidence and credentials do not belong in source control.

## Runner interfaces

- `lessons/first-otlp-pipeline/labs/run.py` (from `lessons/first-otlp-pipeline`): run with `--help` for its required arguments.
- `lessons/read-the-evidence/validation/run.py` (from `lessons/read-the-evidence`): run with `--help` for its required arguments.

Native runners use the pinned Collector; Kubernetes runners require an explicit
`--kubeconfig` and `--context`. Kafka requires its pinned image and an explicit
Docker context. Run only against lab-owned local resources.
