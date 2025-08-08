use crate::prelude::*;
use std::future::Future;
use tokio_cron_scheduler::{JobScheduler, Job};

// ============== PIPELINE SCHEDULER
#[derive(Clone)]
pub struct PipelineScheduler {
    scheduler: JobScheduler,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum SchedulerError {
    InvalidTime(String),
    InvalidFrequency(String),
    JobFailed(String),
}

impl PipelineScheduler {
    /// Creates new Pipeline Scheduler
    pub async fn new<F, Fut>(frequency: &str, job: F) -> ElusionResult<Self> 
    where
    F: Fn() -> Fut + Send + Sync + 'static,
   Fut: Future<Output = ElusionResult<()>> + Send + 'static
{
    println!("Initializing JobScheduler");

    let scheduler = JobScheduler::new().await
        .map_err(|e| ElusionError::Custom(format!("Scheduler init failed: {}", e)))?;
    println!("Jobs are scheduled, and will run with frequency: '{}'", frequency);
        
    let cron = Self::parse_schedule(frequency)?;
    // debug!("Cron expression: {}", cron);

    let job_fn = Arc::new(job);

    let job = Job::new_async(&cron, move |uuid, mut l| {
        let job_fn = job_fn.clone();
        Box::pin(async move {
            let future = job_fn();
            future.await.unwrap_or_else(|e| eprintln!("❌ Job execution failed: {}", e));
            
            let next_tick = l.next_tick_for_job(uuid).await;
            match next_tick {
                Ok(Some(ts)) => println!("Next job execution: {:?} UTC Time", ts),
                _ => println!("Could not determine next job execution"),
            }
        })
    }).map_err(|e| ElusionError::Custom(format!("❌ Job creation failed: {}", e)))?;


        scheduler.add(job).await
            .map_err(|e| ElusionError::Custom(format!("❌ Job scheduling failed: {}", e)))?;
            
        scheduler.start().await
            .map_err(|e| ElusionError::Custom(format!("❌ Scheduler start failed: {}", e)))?;
        
       println!("JobScheduler successfully initialized and started.");

        Ok(Self { scheduler })
    }

    fn parse_schedule(frequency: &str) -> ElusionResult<String> {
        let cron = match frequency.to_lowercase().as_str() {
            "1min" => "0 */1 * * * *".to_string(),
            "2min" => "0 */2 * * * *".to_string(),
            "5min" => "0 */5 * * * *".to_string(),
            "10min" => "0 */10 * * * *".to_string(),
            "15min" => "0 */15 * * * *".to_string(),
            "30min" => "0 */30 * * * *".to_string(),
            "1h" => "0 0 * * * *".to_string(),
            "2h" => "0 0 */2 * * *".to_string(),
            "3h" => "0 0 */3 * * *".to_string(),
            "4h" => "0 0 */4 * * *".to_string(),
            "5h" => "0 0 */5 * * *".to_string(),
            "6h" => "0 0 */6 * * *".to_string(),
            "7h" => "0 0 */7 * * *".to_string(),
            "8h" => "0 0 */8 * * *".to_string(),
            "9h" => "0 0 */9 * * *".to_string(),
            "10h" => "0 0 */10 * * *".to_string(),
            "11h" => "0 0 */11 * * *".to_string(),
            "12h" => "0 0 */12 * * *".to_string(),
            "24h" => "0 0 0 * * *".to_string(),
            "2days" => "0 0 0 */2 * *".to_string(),
            "3days" => "0 0 0 */3 * *".to_string(),
            "4days" => "0 0 0 */4 * *".to_string(),
            "5days" => "0 0 0 */5 * *".to_string(),
            "6days" => "0 0 0 */6 * *".to_string(),
            "7days" => "0 0 0 */7 * *".to_string(),
            "14days" => "0 0 0 */14 * *".to_string(),
            "30days" => "0 0 1 */1 * *".to_string(),
            _ => return Err(ElusionError::Custom(
                "Invalid frequency. Use: 1min,2min,5min,10min,15min,30min,
                1h,2h,3h,4h,5h,6h,7h,8h,9h,10h,11h,12h,24h,
                2days,3days,4days,5days,6days,7days,14days,30days".into()
            ))
        };

        Ok(cron)
    }
    /// Shuts down pipeline job execution
    pub async fn shutdown(mut self) -> ElusionResult<()> {
        println!("Shutdown is ready if needed with -> Ctr+C");
        tokio::signal::ctrl_c().await
            .map_err(|e| ElusionError::Custom(format!("❌ Ctrl+C handler failed: {}", e)))?;
        self.scheduler.shutdown().await
            .map_err(|e| ElusionError::Custom(format!("❌ Shutdown failed: {}", e)))
    }
}