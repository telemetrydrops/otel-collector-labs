# Transform telemetry without losing evidence runnable files

Python 3.11+ and Collector Contrib 0.163.1. Use only the supplied synthetic data for the redaction cases.

Extract this complete archive into the shared course workspace, preserving paths.
Learning instructions and expected observations are provided in the enrolled
course app. Generated evidence and credentials do not belong in source control.

## Runner interfaces

- `lessons/ottl-execution-model/labs/run.py` (from `lessons/ottl-execution-model`): run with `--help` for its required arguments.
- `lessons/normalize-attributes/labs/run.py` (from `lessons/normalize-attributes`): run with `--help` for its required arguments.
- `lessons/remove-noise/labs/run.py` (from `lessons/remove-noise`): run with `--help` for its required arguments.
- `lessons/redact-sensitive-values/labs/run.py` (from `lessons/redact-sensitive-values`): run with `--help` for its required arguments.

Native runners use the pinned Collector; Kubernetes runners require an explicit
`--kubeconfig` and `--context`. Kafka requires its pinned image and an explicit
Docker context. Run only against lab-owned local resources.
