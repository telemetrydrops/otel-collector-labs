# Diagnose the Collector from observed evidence runnable files

Python 3.11+ and Collector Contrib 0.163.1; the observer/destinations run locally.

Extract this complete archive into the shared course workspace, preserving paths.
Learning instructions and expected observations are provided in the enrolled
course app. Generated evidence and credentials do not belong in source control.

## Runner interfaces

- `lessons/collector-self-telemetry/labs/run.py` (from `lessons/collector-self-telemetry`): run with `--help` for its required arguments.
- `lessons/health-and-readiness/labs/run.py` (from `lessons/health-and-readiness`): run with `--help` for its required arguments.
- `lessons/repeatable-troubleshooting/labs/run.py` (from `lessons/repeatable-troubleshooting`): run with `--help` for its required arguments.
- `lessons/profile-and-right-size/labs/run.py` (from `lessons/profile-and-right-size`): run with `--help` for its required arguments.

Native runners use the pinned Collector; Kubernetes runners require an explicit
`--kubeconfig` and `--context`. Kafka requires its pinned image and an explicit
Docker context. Run only against lab-owned local resources.
