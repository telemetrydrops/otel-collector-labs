# Measure and reduce telemetry deliberately runnable files

Python 3.11+ and Collector Contrib 0.163.1. Keep the declared synthetic populations and timing controls unchanged.

Extract this complete archive into the shared course workspace, preserving paths.
Learning instructions and expected observations are provided in the enrolled
course app. Generated evidence and credentials do not belong in source control.

## Runner interfaces

- `lessons/find-volume-before-cutting/labs/run.py` (from `lessons/find-volume-before-cutting`): run with `--help` for its required arguments.
- `lessons/log-volume-reduction/labs/run.py` (from `lessons/log-volume-reduction`): run with `--help` for its required arguments.
- `lessons/head-and-probabilistic-sampling/labs/run.py` (from `lessons/head-and-probabilistic-sampling`): run with `--help` for its required arguments.
- `lessons/tail-sampling-policies/labs/run.py` (from `lessons/tail-sampling-policies`): run with `--help` for its required arguments.

Native runners use the pinned Collector; Kubernetes runners require an explicit
`--kubeconfig` and `--context`. Kafka requires its pinned image and an explicit
Docker context. Run only against lab-owned local resources.
