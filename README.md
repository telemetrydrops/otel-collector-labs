# OpenTelemetry Collector course labs

Public runnable files for **OpenTelemetry Collector: From First Pipeline to Production**.
The course app owns private teaching instructions, guided steps, transcripts and
assessments. These bundles contain runnable code, synthetic input fixtures and
technical file indexes only.

## Module bundles

Each module has one ZIP. Extract it into the shared course workspace without
flattening the `lessons/` paths; existing lesson runner interfaces remain valid.
The course pins the release, full source commit, SHA-256 and byte size. Use the
matching version rather than current main. Keep generated evidence and
credentials outside source control.

| Module | Runnable files |
| --- | --- |
| Prepare your lab environment | [File index](lessons/prepare-lab-environment/labs/README.md) |
| Build and diagnose your first pipeline | [File index](lessons/think-in-pipelines-lab/labs/README.md) |
| Compose and validate a configuration | [File index](lessons/understandable-configurations-lab/labs/README.md) |
| Follow three signals to their destination | [File index](lessons/receive-deliver-signals-lab/labs/README.md) |
| Transform telemetry without losing evidence | [File index](lessons/transform-filter-ottl-lab/labs/README.md) |
| Connect pipelines and verify derived measurements | [File index](lessons/connect-signals-derive-telemetry-lab/labs/README.md) |
| Measure and reduce telemetry deliberately | [File index](lessons/control-telemetry-volume-lab/labs/README.md) |
| Secure and isolate local telemetry routes | [File index](lessons/secure-isolate-pipelines-lab/labs/README.md) |
| Follow telemetry through failures and recovery | [File index](lessons/failure-backpressure-lab/labs/README.md) |
| Diagnose the Collector from observed evidence | [File index](lessons/observe-troubleshoot-lab/labs/README.md) |
| Operate Collector topologies in a disposable cluster | [File index](lessons/deploy-on-kubernetes-lab/labs/README.md) |
| Scale routes while preserving state ownership | [File index](lessons/scale-collector-work-lab/labs/README.md) |
| Build, test and defend a production pipeline | [File index](lessons/production-pipeline-capstone-lab/labs/README.md) |

## Requirements

Native cases use official Collector Contrib **0.163.1** and Python **3.11+**
without Python packages or a cloud account. The configuration module also
compares the matching core Collector. Security and capstone cases require
OpenSSL. The Kafka case requires its pinned Apache Kafka image and an explicit
local Docker context. Kubernetes/target-allocation cases require kubectl, k3d,
the GitHub CLI (`gh`), Docker and an explicitly selected disposable kubeconfig/context; their topology
setup script does not select the default kubectl context.

Linux AMD64 is verified for the native, Kafka, capstone and disposable-cluster
runner executions. The existing setup/first-pipeline pilots retain their prior
macOS Apple Silicon verification. Check the module index and each runner's
`--help` before execution. A local run does not establish production capacity,
general exactly-once delivery or cloud deployment support.

## Versioned downloads

Build deterministic archives into a new output directory:

```sh
python3 scripts/package.py --output /tmp/collector-module-release
python3 -m unittest discover -s tests
```

`modules.json` explicitly lists each bundle's files. Packaging rejects escaping
paths, symlinks, private documents, and non-code validation receipts. The
manifest records each archive's SHA-256 and size. The two original pilot asset
names remain available with unchanged pilot bytes. Never replace an asset
already referenced by a course; publish a new immutable release instead.
