# Secure and isolate local telemetry routes runnable files

Python 3.11+, OpenSSL and Collector Contrib 0.163.1. Certificates and credentials are generated locally by the runners; never substitute production credentials.

Extract this complete archive into the shared course workspace, preserving paths.
Learning instructions and expected observations are provided in the enrolled
course app. Generated evidence and credentials do not belong in source control.

## Runner interfaces

- `lessons/telemetry-threat-model/labs/run.py` (from `lessons/telemetry-threat-model`): run with `--help` for its required arguments.
- `lessons/tls-both-directions/labs/run.py` (from `lessons/tls-both-directions`): run with `--help` for its required arguments.
- `lessons/authenticate-clients-destinations/labs/run.py` (from `lessons/authenticate-clients-destinations`): run with `--help` for its required arguments.
- `lessons/route-tenants-safely/labs/run.py` (from `lessons/route-tenants-safely`): run with `--help` for its required arguments.

Native runners use the pinned Collector; Kubernetes runners require an explicit
`--kubeconfig` and `--context`. Kafka requires its pinned image and an explicit
Docker context. Run only against lab-owned local resources.
