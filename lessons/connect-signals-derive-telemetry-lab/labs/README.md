# Connect pipelines and verify derived measurements runnable files

Python 3.11+ and Collector Contrib 0.163.1; no account or remote backend is required.

Extract this complete archive into the shared course workspace, preserving paths.
Learning instructions and expected observations are provided in the enrolled
course app. Generated evidence and credentials do not belong in source control.

## Runner interfaces

- `lessons/connector-boundaries/validation/run.py` (from `lessons/connector-boundaries`): run with `--help` for its required arguments.
- `lessons/spans-to-service-metrics/validation/run.py` (from `lessons/spans-to-service-metrics`): run with `--help` for its required arguments.
- `lessons/logs-to-metrics/validation/run.py` (from `lessons/logs-to-metrics`): run with `--help` for its required arguments.
- `lessons/measure-before-sampling/validation/run.py` (from `lessons/measure-before-sampling`): run with `--help` for its required arguments.

Native runners use the pinned Collector; Kubernetes runners require an explicit
`--kubeconfig` and `--context`. Kafka requires its pinned image and an explicit
Docker context. Run only against lab-owned local resources.
