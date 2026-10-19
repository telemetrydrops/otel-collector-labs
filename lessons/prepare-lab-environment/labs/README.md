# Prepare lab environment

Runnable files for the Telemetry Drops Collector course. Use official Collector
Contrib **0.163.1** and Python **3.11+**. The pilot is verified on macOS Apple
Silicon; other platforms are not yet verified. Course teaching instructions and
assessments are delivered separately in the learning app.

Extract the lesson archive into `~/telemetry-drops/collector-labs`, keeping its
`lessons/` tree. Keep your Collector executable at `bin/otelcol-contrib` in that
workspace. From `lessons/prepare-lab-environment` run:

```sh
python3 labs/check.py --workspace "$HOME/telemetry-drops/collector-labs"
```

Keep a new evidence directory for each first-pipeline run. The setup checker
creates a fresh directory automatically. Both runners bind only to loopback
and stop only their own Collector process. Local check output is not an app
completion record. Inspect the Python source and YAML before running them.
