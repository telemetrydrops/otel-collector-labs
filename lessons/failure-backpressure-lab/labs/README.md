# Follow telemetry through failures and recovery runnable files

Python 3.11+ and Collector Contrib 0.163.1. The Kafka step also requires Docker with an explicitly selected context and its pinned broker image; inspect that step before running it.

Extract this complete archive into the shared course workspace, preserving paths.
Learning instructions and expected observations are provided in the enrolled
course app. Generated evidence and credentials do not belong in source control.

## Runner interfaces

- `lessons/failed-export-backpressure/validation/run.py` (from `lessons/failed-export-backpressure`): run with `--help` for its required arguments.
- `lessons/size-memory-queues/validation/run.py` (from `lessons/size-memory-queues`): run with `--help` for its required arguments.
- `lessons/persist-sending-queue/validation/run.py` (from `lessons/persist-sending-queue`): run with `--help` for its required arguments.
- `lessons/decouple-with-kafka/validation/run.py` (from `lessons/decouple-with-kafka`): run with `--help` for its required arguments.

Native runners use the pinned Collector; Kubernetes runners require an explicit
`--kubeconfig` and `--context`. Kafka requires its pinned image and an explicit
Docker context. Run only against lab-owned local resources.
