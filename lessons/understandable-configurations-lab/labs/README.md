# Compose and validate a configuration runnable files

Python 3.11+, Collector Contrib 0.163.1 and the matching official core Collector for the distribution comparison. Follow the distribution step to verify both downloads.

Extract this complete archive into the shared course workspace, preserving paths.
Learning instructions and expected observations are provided in the enrolled
course app. Generated evidence and credentials do not belong in source control.

## Runner interfaces

- `lessons/enabled-pipelines/validation/run.py` (from `lessons/enabled-pipelines`): run with `--help` for its required arguments.
- `lessons/choose-distribution/validation/run.py` (from `lessons/choose-distribution`): run with `--help` for its required arguments.
- `lessons/compose-configuration-safely/validation/run.py` (from `lessons/compose-configuration-safely`): run with `--help` for its required arguments.
- `lessons/validate-before-running/validation/run.py` (from `lessons/validate-before-running`): run with `--help` for its required arguments.

Native runners use the pinned Collector; Kubernetes runners require an explicit
`--kubeconfig` and `--context`. Kafka requires its pinned image and an explicit
Docker context. Run only against lab-owned local resources.
