# Follow three signals to their destination runnable files

Python 3.11+ and Collector Contrib 0.163.1. The runners provide local synthetic senders and destinations.

Extract this complete archive into the shared course workspace, preserving paths.
Learning instructions and expected observations are provided in the enrolled
course app. Generated evidence and credentials do not belong in source control.

## Runner interfaces

- `lessons/otlp-without-mystery/labs/run.py` (from `lessons/otlp-without-mystery`): run with `--help` for its required arguments.
- `lessons/application-telemetry/labs/run.py` (from `lessons/application-telemetry`): run with `--help` for its required arguments.
- `lessons/infrastructure-file-telemetry/labs/run.py` (from `lessons/infrastructure-file-telemetry`): run with `--help` for its required arguments.
- `lessons/export-and-verify/labs/run.py` (from `lessons/export-and-verify`): run with `--help` for its required arguments.

Native runners use the pinned Collector; Kubernetes runners require an explicit
`--kubeconfig` and `--context`. Kafka requires its pinned image and an explicit
Docker context. Run only against lab-owned local resources.
