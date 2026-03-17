use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::parser::{CronParser, Seconds};
use serde::{Deserialize, Serialize};
use tempfile::{NamedTempFile, TempPath};
use thiserror::Error;
use tokio::process::Command;
use tokio::signal;
use tokio_cron_scheduler::{Job, JobScheduler};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub timezone: String,
    pub jobs: Vec<JobConfig>,
}

#[derive(Debug, Deserialize)]
pub struct JobConfig {
    pub name: String,
    pub schedule: String,
    pub function: String,
    pub payload: Option<PayloadConfig>,
    pub timeout_seconds: Option<u64>,
    pub overlap_policy: Option<OverlapPolicy>,
}

#[derive(Debug, Deserialize)]
pub struct PayloadConfig {
    pub file: Option<PathBuf>,
    pub generate_eventbridge_scheduled_event: Option<bool>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum OverlapPolicy {
    Allow,
    #[default]
    Forbid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedConfig {
    pub timezone: Tz,
    pub jobs: Vec<Arc<JobDefinition>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobDefinition {
    pub name: String,
    pub schedule: String,
    pub function: String,
    pub payload: JobPayload,
    pub timeout: Option<Duration>,
    pub overlap_policy: OverlapPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobPayload {
    None,
    File(PathBuf),
    GeneratedEventBridge,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file `{path}`")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse YAML config")]
    Parse(#[source] serde_yaml::Error),
    #[error("timezone `{0}` is invalid")]
    InvalidTimezone(String),
    #[error("job `{job}` has an invalid schedule `{schedule}`; MVP requires exactly 6 cron fields")]
    InvalidSchedule { job: String, schedule: String },
    #[error(
        "job `{0}` payload must choose exactly one of `file` or `generate_eventbridge_scheduled_event: true`"
    )]
    InvalidPayload(String),
    #[error("job `{job}` payload file does not exist: {path}")]
    MissingPayloadFile { job: String, path: PathBuf },
}

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("failed to prepare invocation")]
    Prepare(#[source] anyhow::Error),
    #[error("failed to spawn cargo lambda process")]
    Spawn(#[source] std::io::Error),
    #[error("failed to wait for cargo lambda process")]
    Wait(#[source] std::io::Error),
    #[error("failed to stop cargo lambda process after timeout")]
    Kill(#[source] std::io::Error),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionContext {
    pub scheduled_at: DateTime<Utc>,
    pub timezone: Tz,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionResult {
    pub status: ExecutionStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionStatus {
    Success(i32),
    Failure(Option<i32>),
    TimedOut,
}

impl ExecutionStatus {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success(_))
    }
}

impl fmt::Display for ExecutionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Success(code) => write!(f, "succeeded (exit code {code})"),
            Self::Failure(Some(code)) => write!(f, "failed (exit code {code})"),
            Self::Failure(None) => f.write_str("failed (terminated by signal)"),
            Self::TimedOut => f.write_str("timed out"),
        }
    }
}

#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute(
        &self,
        job: &JobDefinition,
        ctx: &ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutionError>;
}

#[derive(Debug, Default)]
pub struct CargoLambdaExecutor;

#[derive(Debug)]
struct PreparedInvocation {
    program: String,
    args: Vec<String>,
    temp_payload: Option<TempPath>,
}

#[derive(Debug)]
struct JobRuntimeState {
    running: AtomicBool,
}

impl JobRuntimeState {
    fn new() -> Self {
        Self {
            running: AtomicBool::new(false),
        }
    }
}

pub async fn run(config_path: &Path) -> Result<()> {
    let config = load_config(config_path)?;
    let executor: Arc<dyn Executor> = Arc::new(CargoLambdaExecutor);

    let mut scheduler = JobScheduler::new()
        .await
        .context("failed to create tokio cron scheduler")?;

    for job in &config.jobs {
        register_job(
            &scheduler,
            Arc::clone(job),
            config.timezone,
            Arc::clone(&executor),
        )
        .await
        .with_context(|| format!("failed to register job `{}`", job.name))?;
    }

    scheduler
        .start()
        .await
        .context("failed to start scheduler")?;

    println!(
        "ryacron started with timezone {} and {} job(s)",
        config.timezone,
        config.jobs.len()
    );

    signal::ctrl_c()
        .await
        .context("failed to listen for Ctrl-C")?;

    println!("shutdown requested, stopping scheduler");
    scheduler
        .shutdown()
        .await
        .context("failed to shut down scheduler")?;
    println!("scheduler stopped");

    Ok(())
}

pub fn load_config(path: &Path) -> std::result::Result<ValidatedConfig, ConfigError> {
    let raw = fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let parsed: AppConfig = serde_yaml::from_str(&raw).map_err(ConfigError::Parse)?;
    parsed.validate(path.parent().unwrap_or_else(|| Path::new(".")))
}

impl AppConfig {
    fn validate(self, base_dir: &Path) -> std::result::Result<ValidatedConfig, ConfigError> {
        let timezone = self
            .timezone
            .parse::<Tz>()
            .map_err(|_| ConfigError::InvalidTimezone(self.timezone.clone()))?;

        let jobs = self
            .jobs
            .into_iter()
            .map(|job| job.validate(base_dir))
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .map(Arc::new)
            .collect();

        Ok(ValidatedConfig { timezone, jobs })
    }
}

impl JobConfig {
    fn validate(self, base_dir: &Path) -> std::result::Result<JobDefinition, ConfigError> {
        validate_schedule(&self.name, &self.schedule)?;

        let payload = match self.payload {
            None => JobPayload::None,
            Some(payload) => payload.validate(&self.name, base_dir)?,
        };

        Ok(JobDefinition {
            name: self.name,
            schedule: self.schedule,
            function: self.function,
            payload,
            timeout: self.timeout_seconds.map(Duration::from_secs),
            overlap_policy: self.overlap_policy.unwrap_or_default(),
        })
    }
}

impl PayloadConfig {
    fn validate(
        self,
        job_name: &str,
        base_dir: &Path,
    ) -> std::result::Result<JobPayload, ConfigError> {
        match (self.file, self.generate_eventbridge_scheduled_event) {
            (Some(path), None | Some(false)) => {
                let resolved = resolve_path(base_dir, path);
                if !resolved.exists() {
                    return Err(ConfigError::MissingPayloadFile {
                        job: job_name.to_owned(),
                        path: resolved,
                    });
                }
                Ok(JobPayload::File(resolved))
            }
            (None, Some(true)) => Ok(JobPayload::GeneratedEventBridge),
            _ => Err(ConfigError::InvalidPayload(job_name.to_owned())),
        }
    }
}

impl CargoLambdaExecutor {
    fn prepare_invocation(
        &self,
        job: &JobDefinition,
        ctx: &ExecutionContext,
    ) -> Result<PreparedInvocation> {
        let mut args = vec![
            "lambda".to_string(),
            "invoke".to_string(),
            job.function.clone(),
        ];
        let mut temp_payload = None;

        match &job.payload {
            JobPayload::None => {}
            JobPayload::File(path) => {
                args.push("--data-file".to_string());
                args.push(path.display().to_string());
            }
            JobPayload::GeneratedEventBridge => {
                let mut temp_file =
                    NamedTempFile::new().context("failed to create temp payload file")?;
                let event = build_eventbridge_event(ctx);
                serde_json::to_writer_pretty(temp_file.as_file_mut(), &event)
                    .context("failed to write generated eventbridge payload")?;
                let temp_path = temp_file.into_temp_path();
                args.push("--data-file".to_string());
                args.push(temp_path.to_path_buf().display().to_string());
                temp_payload = Some(temp_path);
            }
        }

        Ok(PreparedInvocation {
            program: "cargo".to_string(),
            args,
            temp_payload,
        })
    }
}

#[async_trait]
impl Executor for CargoLambdaExecutor {
    async fn execute(
        &self,
        job: &JobDefinition,
        ctx: &ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutionError> {
        let prepared = self
            .prepare_invocation(job, ctx)
            .map_err(ExecutionError::Prepare)?;

        let mut command = Command::new(&prepared.program);
        command.args(&prepared.args);

        let mut child = command.spawn().map_err(ExecutionError::Spawn)?;
        let status = match job.timeout {
            Some(timeout) => match tokio::time::timeout(timeout, child.wait()).await {
                Ok(wait_result) => wait_result.map_err(ExecutionError::Wait)?,
                Err(_) => {
                    child.kill().await.map_err(ExecutionError::Kill)?;
                    child.wait().await.map_err(ExecutionError::Wait)?;
                    drop(prepared.temp_payload);
                    return Ok(ExecutionResult {
                        status: ExecutionStatus::TimedOut,
                    });
                }
            },
            None => child.wait().await.map_err(ExecutionError::Wait)?,
        };

        drop(prepared.temp_payload);

        let execution_status = if status.success() {
            ExecutionStatus::Success(status.code().unwrap_or_default())
        } else {
            ExecutionStatus::Failure(status.code())
        };

        Ok(ExecutionResult {
            status: execution_status,
        })
    }
}

async fn register_job(
    scheduler: &JobScheduler,
    job: Arc<JobDefinition>,
    timezone: Tz,
    executor: Arc<dyn Executor>,
) -> Result<()> {
    let state = Arc::new(JobRuntimeState::new());
    let schedule = job.schedule.clone();
    let registered = Job::new_async_tz(schedule, timezone, move |_job_id, _jobs| {
        let job = Arc::clone(&job);
        let executor = Arc::clone(&executor);
        let state = Arc::clone(&state);

        Box::pin(async move {
            run_scheduled_job(job, timezone, executor, state).await;
        }) as Pin<Box<dyn std::future::Future<Output = ()> + Send>>
    })
    .context("failed to create cron job")?;

    scheduler
        .add(registered)
        .await
        .context("failed to add cron job to scheduler")?;

    Ok(())
}

async fn run_scheduled_job(
    job: Arc<JobDefinition>,
    timezone: Tz,
    executor: Arc<dyn Executor>,
    state: Arc<JobRuntimeState>,
) {
    if job.overlap_policy == OverlapPolicy::Forbid
        && state
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
    {
        println!("job={} status=skipped reason=overlap_forbidden", job.name);
        return;
    }

    let _guard = RunningGuard::new(
        job.overlap_policy == OverlapPolicy::Forbid,
        Arc::clone(&state),
    );
    let scheduled_at = Utc::now();
    let ctx = ExecutionContext {
        scheduled_at,
        timezone,
    };

    println!(
        "job={} status=started scheduled_at={}",
        job.name,
        scheduled_at.with_timezone(&timezone).to_rfc3339()
    );

    match executor.execute(&job, &ctx).await {
        Ok(result) if result.status.is_success() => {
            println!("job={} status={}", job.name, result.status);
        }
        Ok(result) => {
            println!("job={} status={}", job.name, result.status);
        }
        Err(err) => {
            eprintln!("job={} status=error error={err}", job.name);
        }
    }
}

struct RunningGuard {
    enabled: bool,
    state: Arc<JobRuntimeState>,
}

impl RunningGuard {
    fn new(enabled: bool, state: Arc<JobRuntimeState>) -> Self {
        Self { enabled, state }
    }
}

impl Drop for RunningGuard {
    fn drop(&mut self) {
        if self.enabled {
            self.state.running.store(false, Ordering::Release);
        }
    }
}

fn validate_schedule(job_name: &str, schedule: &str) -> std::result::Result<(), ConfigError> {
    if schedule.split_whitespace().count() != 6 {
        return Err(ConfigError::InvalidSchedule {
            job: job_name.to_owned(),
            schedule: schedule.to_owned(),
        });
    }

    CronParser::builder()
        .seconds(Seconds::Required)
        .dom_and_dow(true)
        .build()
        .parse(schedule)
        .map_err(|_| ConfigError::InvalidSchedule {
            job: job_name.to_owned(),
            schedule: schedule.to_owned(),
        })?;

    Ok(())
}

fn resolve_path(base_dir: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
    }
}

#[derive(Debug, Serialize)]
struct EventBridgeEvent {
    id: String,
    time: String,
    source: String,
    #[serde(rename = "detail-type")]
    detail_type: String,
    resources: Vec<String>,
    detail: serde_json::Value,
}

fn build_eventbridge_event(ctx: &ExecutionContext) -> EventBridgeEvent {
    EventBridgeEvent {
        id: Uuid::new_v4().to_string(),
        time: ctx.scheduled_at.with_timezone(&ctx.timezone).to_rfc3339(),
        source: "dev.ryacron".to_string(),
        detail_type: "Scheduled Event".to_string(),
        resources: Vec::new(),
        detail: serde_json::json!({}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tempfile::tempdir;
    use tokio::sync::Notify;

    #[test]
    fn loads_valid_yaml() {
        let dir = tempdir().unwrap();
        let payload_path = dir.path().join("event.json");
        fs::write(&payload_path, "{}").unwrap();
        let config_path = dir.path().join("ryacron.yaml");
        fs::write(
            &config_path,
            format!(
                "timezone: Asia/Tokyo\njobs:\n  - name: ok\n    schedule: \"0 * * * * *\"\n    function: demo\n    payload:\n      file: {}\n",
                payload_path.display()
            ),
        )
        .unwrap();

        let config = load_config(&config_path).unwrap();

        assert_eq!(config.timezone, chrono_tz::Asia::Tokyo);
        assert_eq!(config.jobs.len(), 1);
        assert_eq!(config.jobs[0].payload, JobPayload::File(payload_path));
    }

    #[test]
    fn rejects_invalid_timezone() {
        let raw = AppConfig {
            timezone: "Mars/Phobos".to_string(),
            jobs: vec![],
        };

        let err = raw.validate(Path::new(".")).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidTimezone(_)));
    }

    #[test]
    fn rejects_five_field_schedule() {
        let raw = AppConfig {
            timezone: "Asia/Tokyo".to_string(),
            jobs: vec![JobConfig {
                name: "bad".to_string(),
                schedule: "* * * * *".to_string(),
                function: "demo".to_string(),
                payload: None,
                timeout_seconds: None,
                overlap_policy: None,
            }],
        };

        let err = raw.validate(Path::new(".")).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidSchedule { .. }));
    }

    #[test]
    fn rejects_ambiguous_payload() {
        let raw = PayloadConfig {
            file: Some(PathBuf::from("event.json")),
            generate_eventbridge_scheduled_event: Some(true),
        };

        let err = raw.validate("job", Path::new(".")).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidPayload(_)));
    }

    #[test]
    fn prepares_file_payload_invocation() {
        let executor = CargoLambdaExecutor;
        let job = JobDefinition {
            name: "job".to_string(),
            schedule: "0 * * * * *".to_string(),
            function: "email-notification-sender".to_string(),
            payload: JobPayload::File(PathBuf::from("/tmp/payload.json")),
            timeout: Some(Duration::from_secs(600)),
            overlap_policy: OverlapPolicy::Forbid,
        };
        let ctx = ExecutionContext {
            scheduled_at: DateTime::parse_from_rfc3339("2026-03-17T12:00:00+09:00")
                .unwrap()
                .with_timezone(&Utc),
            timezone: chrono_tz::Asia::Tokyo,
        };

        let prepared = executor.prepare_invocation(&job, &ctx).unwrap();

        assert_eq!(prepared.program, "cargo");
        assert_eq!(
            prepared.args,
            vec![
                "lambda",
                "invoke",
                "email-notification-sender",
                "--data-file",
                "/tmp/payload.json",
            ]
        );
    }

    #[test]
    fn prepares_generated_eventbridge_payload() {
        let executor = CargoLambdaExecutor;
        let job = JobDefinition {
            name: "job".to_string(),
            schedule: "0 */5 * * * *".to_string(),
            function: "batch-job".to_string(),
            payload: JobPayload::GeneratedEventBridge,
            timeout: Some(Duration::from_secs(600)),
            overlap_policy: OverlapPolicy::Forbid,
        };
        let ctx = ExecutionContext {
            scheduled_at: DateTime::parse_from_rfc3339("2026-03-17T12:34:56+09:00")
                .unwrap()
                .with_timezone(&Utc),
            timezone: chrono_tz::Asia::Tokyo,
        };

        let prepared = executor.prepare_invocation(&job, &ctx).unwrap();
        let payload_path = PathBuf::from(prepared.args.last().unwrap());
        let payload = fs::read_to_string(payload_path).unwrap();
        let json: serde_json::Value = serde_json::from_str(&payload).unwrap();

        assert_eq!(json["detail-type"], "Scheduled Event");
        assert_eq!(json["source"], "dev.ryacron");
        assert_eq!(json["time"], "2026-03-17T12:34:56+09:00");
        drop(prepared);
    }

    #[tokio::test]
    async fn forbid_overlap_skips_second_run() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let executor = Arc::new(BlockedExecutor {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            calls: Arc::new(Mutex::new(0)),
        });
        let executor_calls = Arc::clone(&executor.calls);
        let job = Arc::new(JobDefinition {
            name: "job".to_string(),
            schedule: "0 * * * * *".to_string(),
            function: "demo".to_string(),
            payload: JobPayload::None,
            timeout: None,
            overlap_policy: OverlapPolicy::Forbid,
        });
        let state = Arc::new(JobRuntimeState::new());

        let first = tokio::spawn(run_scheduled_job(
            Arc::clone(&job),
            chrono_tz::Asia::Tokyo,
            executor,
            Arc::clone(&state),
        ));

        started.notified().await;

        run_scheduled_job(
            Arc::clone(&job),
            chrono_tz::Asia::Tokyo,
            Arc::new(NoopExecutor),
            Arc::clone(&state),
        )
        .await;

        release.notify_waiters();
        first.await.unwrap();

        assert_eq!(*executor_calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn allow_overlap_runs_both() {
        let calls = Arc::new(Mutex::new(0));
        let job = Arc::new(JobDefinition {
            name: "job".to_string(),
            schedule: "0 * * * * *".to_string(),
            function: "demo".to_string(),
            payload: JobPayload::None,
            timeout: None,
            overlap_policy: OverlapPolicy::Allow,
        });
        let state = Arc::new(JobRuntimeState::new());
        let executor: Arc<dyn Executor> = Arc::new(CountingExecutor {
            calls: Arc::clone(&calls),
        });

        let first = tokio::spawn(run_scheduled_job(
            Arc::clone(&job),
            chrono_tz::Asia::Tokyo,
            Arc::clone(&executor),
            Arc::clone(&state),
        ));
        let second = tokio::spawn(run_scheduled_job(
            Arc::clone(&job),
            chrono_tz::Asia::Tokyo,
            executor,
            Arc::clone(&state),
        ));

        first.await.unwrap();
        second.await.unwrap();

        assert_eq!(*calls.lock().unwrap(), 2);
    }

    struct CountingExecutor {
        calls: Arc<Mutex<i32>>,
    }

    #[async_trait]
    impl Executor for CountingExecutor {
        async fn execute(
            &self,
            _job: &JobDefinition,
            _ctx: &ExecutionContext,
        ) -> std::result::Result<ExecutionResult, ExecutionError> {
            *self.calls.lock().unwrap() += 1;
            Ok(ExecutionResult {
                status: ExecutionStatus::Success(0),
            })
        }
    }

    struct NoopExecutor;

    #[async_trait]
    impl Executor for NoopExecutor {
        async fn execute(
            &self,
            _job: &JobDefinition,
            _ctx: &ExecutionContext,
        ) -> std::result::Result<ExecutionResult, ExecutionError> {
            Ok(ExecutionResult {
                status: ExecutionStatus::Success(0),
            })
        }
    }

    struct BlockedExecutor {
        started: Arc<Notify>,
        release: Arc<Notify>,
        calls: Arc<Mutex<i32>>,
    }

    #[async_trait]
    impl Executor for BlockedExecutor {
        async fn execute(
            &self,
            _job: &JobDefinition,
            _ctx: &ExecutionContext,
        ) -> std::result::Result<ExecutionResult, ExecutionError> {
            *self.calls.lock().unwrap() += 1;
            self.started.notify_waiters();
            self.release.notified().await;
            Ok(ExecutionResult {
                status: ExecutionStatus::Success(0),
            })
        }
    }
}
