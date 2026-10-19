# OpenTelemetry Collector course labs

Public runnable files for the Telemetry Drops course **OpenTelemetry Collector:
From First Pipeline to Production**. Course scripts, transcripts, quizzes and
assessment keys are maintained separately in the private editorial repository.

## Pilot labs

- [Prepare your lab environment](lessons/prepare-lab-environment/labs/README.md)
  checks the pinned Collector, configuration, loopback receiver and shutdown.
- [Your first OTLP pipeline](lessons/first-otlp-pipeline/labs/README.md) verifies
  real exported spans and malformed-input rejection using the same workspace.

Use official Collector Contrib **0.163.1** and Python **3.11+**, with no Python
packages or cloud account. macOS Apple Silicon is the verified pilot platform.
Installation instructions and the guided learning experience belong in the
course app. Runtime evidence, credentials, student data, and generated archives
are not committed here.

## Versioned downloads

Release assets package each lab under `lessons/<lesson-id>/labs/`. Extract them
into a shared workspace without flattening paths. Course references pin a source
commit and an archive SHA-256; use the matching release rather than current main.

Build deterministic assets into a new local directory:

```sh
python3 scripts/package.py --output /tmp/collector-lab-release
```

The package manifest identifies each archive's SHA-256 and byte length. Only the
explicit pilot file list is packaged. The source commit is recorded separately
when creating the release. Reviewed archives are uploaded through `gh release`;
never replace the bytes of an asset already referenced by a course.
