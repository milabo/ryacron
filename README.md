# ryacron

`ryacron` is a Rust-based cron runner inspired by [`yacron`](https://github.com/gjcarneiro/yacron).

The current MVP focuses on a practical local use case: acting as a lightweight EventBridge emulator that invokes Cargo Lambda functions on a cron schedule.

## Status

This project is intentionally small for now.

- YAML-driven job definitions
- Global timezone per config file
- 6-field cron expressions via `tokio-cron-scheduler`
- Cargo Lambda execution backend
- Payload from file or generated EventBridge-like event
- Per-job timeout
- Per-job max concurrent runs with skip-on-limit behavior

The long-term direction is to keep the project name and config model generic while allowing more execution backends in the future.

## Installation

```bash
cargo build
```

## Usage

Run the scheduler with a YAML config file:

```bash
cargo run -- run --config ./ryacron.yaml
```

If you already built the binary:

```bash
./target/debug/ryacron run --config ./ryacron.yaml
```

The process keeps running until you stop it with `Ctrl-C`.

## Configuration

Example:

```yaml
timezone: Asia/Tokyo

jobs:
  - name: batch_job_a
    schedule: "0 * * * * *"
    function: batch_job_q
    payload:
      file: examples/eventbridge.json
    timeout_seconds: 600
    max_concurrent_runs: 1

  - name: batch_job_b
    schedule: "0 */5 * * * *"
    function: batch_job_b
    payload:
      generate_eventbridge_scheduled_event: true
    timeout_seconds: 600
    max_concurrent_runs: 5
```

### Top-level fields

- `timezone`: IANA timezone name such as `Asia/Tokyo` or `UTC`
- `jobs`: list of scheduled jobs

### Job fields

- `name`: human-readable job name
- `schedule`: cron expression in `sec min hour dom mon dow` format
- `function`: Cargo Lambda function name passed to `cargo lambda invoke`
- `payload`: optional payload source
- `timeout_seconds`: optional timeout in seconds
- `max_concurrent_runs`: optional positive integer, defaults to `1`

### Payload modes

`payload` may specify exactly one of the following:

```yaml
payload:
  file: path/to/event.json
```

or

```yaml
payload:
  generate_eventbridge_scheduled_event: true
```

If `payload` is omitted, `ryacron` invokes the function without `--data-file`.

## Concurrency control

`max_concurrent_runs` controls how many invocations of the same job may run at once.

- If omitted, the default is `1`
- If the job is already running `max_concurrent_runs` times, the next scheduled invoke is skipped
- `1` matches the old "no overlap" behavior

Example:

```yaml
jobs:
  - name: batch_job
    schedule: "0 * * * * *"
    function: batch-job
    max_concurrent_runs: 5
```

In this example, up to 5 concurrent `cargo lambda invoke` processes may run for `batch_job`. If 5 are already running, the next tick is skipped.

## Cron format

`ryacron` currently requires exactly 6 cron fields:

```text
sec min hour day-of-month month day-of-week
```

Examples:

- `"0 * * * * *"`: every minute
- `"0 */5 * * * *"`: every 5 minutes
- `"30 0 9 * * 1-5"`: 09:00:30 on weekdays

5-field cron expressions are rejected in the current MVP.

## EventBridge-like generated payload

When `generate_eventbridge_scheduled_event: true` is used, `ryacron` generates a minimal event payload with these fields:

- `id`
- `time`
- `source`
- `detail-type`
- `resources`
- `detail`

This is meant for local development and testing, not strict AWS EventBridge parity.

## Validation rules

At startup, `ryacron` validates:

- YAML syntax
- timezone value
- cron format
- payload shape
- payload file existence
- `max_concurrent_runs >= 1`

Invalid configuration causes startup to fail with a non-zero exit code.

## Logging behavior

The scheduler writes simple human-readable logs to standard output and standard error, including:

- scheduler start and stop
- job start
- job success or failure
- timeout
- overlap skip

## Current limitations

- only one executor backend: Cargo Lambda CLI
- no hot reload
- no separate `validate` command yet
- no per-job timezone
- no persistent execution history
- no strict `yacron` compatibility guarantee

## Development

Format and test locally:

```bash
cargo fmt
cargo test
```

Show CLI help:

```bash
cargo run -- --help
```

## License

This project is licensed under the terms of the [LICENSE](LICENSE) file.
