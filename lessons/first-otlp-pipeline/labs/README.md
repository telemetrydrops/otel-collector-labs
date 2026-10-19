# First otlp pipeline

Runnable files for the Telemetry Drops Collector course. Use official Collector
Contrib **0.163.1** and Python **3.11+**. The pilot is verified on macOS Apple
Silicon; other platforms are not yet verified. Course teaching instructions and
assessments are delivered separately in the learning app.

Extract the lesson archive into `~/telemetry-drops/collector-labs`, keeping its
`lessons/` tree. Keep your Collector executable at `bin/otelcol-contrib` in that
workspace. From `lessons/first-otlp-pipeline` run:

```sh
python3 labs/run.py --collector "$HOME/telemetry-drops/collector-labs/bin/otelcol-contrib" --evidence "$HOME/telemetry-drops/collector-labs/evidence/first-otlp-run"
```

Keep a new evidence directory for each first-pipeline run. The setup checker
creates a fresh directory automatically. Both runners bind only to loopback
and stop only their own Collector process. Local check output is not an app
completion record. Inspect the Python source and YAML before running them.
