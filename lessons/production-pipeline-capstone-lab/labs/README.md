# Build, test and defend a production pipeline runnable files

Python 3.11+, OpenSSL and Collector Contrib 0.163.1. The composed runner uses local synthetic traffic; no production account is required.

Extract this complete archive into the shared course workspace, preserving paths.
Learning instructions and expected observations are provided in the enrolled
course app. Generated evidence and credentials do not belong in source control.

## Runner interfaces

- `lessons/build-production-topology/labs/run.py` (from `lessons/build-production-topology`): run with `--help` for its required arguments.

Native runners use the pinned Collector; Kubernetes runners require an explicit
`--kubeconfig` and `--context`. Kafka requires its pinned image and an explicit
Docker context. Run only against lab-owned local resources.
