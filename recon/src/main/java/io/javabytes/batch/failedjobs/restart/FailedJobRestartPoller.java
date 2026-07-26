package io.javabytes.batch.failedjobs.restart;

import java.util.List;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class FailedJobRestartPoller {

    private final JobLauncher jobLauncher;
    private final JobExplorer jobExplorer;
    private final List<Job> registeredJobs; // Spring injects ALL @Bean Job instances

    @Value("${batch.restart.max-retries:3}")
    private int maxRetries;

    @Value("${batch.restart.lookback-instances:5}")
    private int lookbackInstances;

    /**
     * Runs every 60 seconds. Configurable via
     * batch.restart.poll-interval-ms in application.properties.
     */
    @Scheduled(fixedDelayString = "${batch.restart.poll-interval-ms:60000}",
               initialDelayString = "${batch.restart.initial-delay-ms:10000}")
    public void pollAndRestartFailedJobs() {
        log.info("=== Polling for FAILED job executions ===");

        for (Job job : registeredJobs) {
            String jobName = job.getName();
            try {
                restartFailedExecutionsFor(job, jobName);
            } catch (Exception e) {
                log.error("Unexpected error while checking job [{}]: {}", jobName, e.getMessage(), e);
            }
        }
    }

    private void restartFailedExecutionsFor(Job job, String jobName) {
        // Pull the most recent N job instances from the repository
        List<JobInstance> instances =
            jobExplorer.getJobInstances(jobName, 0, lookbackInstances);

        for (JobInstance instance : instances) {
            List<JobExecution> executions =
                jobExplorer.getJobExecutions(instance);

            // Most recent execution is first
            JobExecution latest = executions.isEmpty() ? null : executions.get(0);
            if (latest == null) continue;

            BatchStatus status = latest.getStatus();

            // Only restart truly FAILED executions
            if (status != BatchStatus.FAILED) {
                log.debug("Job [{}] instance [{}] status is {} — skipping",
                    jobName, instance.getInstanceId(), status);
                continue;
            }

            // Guard: check if any execution for this instance is already running
            boolean alreadyRunning = executions.stream()
                .anyMatch(e -> e.getStatus() == BatchStatus.STARTED
                            || e.getStatus() == BatchStatus.STARTING
                            || e.getStatus() == BatchStatus.STOPPING);

            if (alreadyRunning) {
                log.info("Job [{}] instance [{}] already has a running execution — skipping restart",
                    jobName, instance.getInstanceId());
                continue;
            }

            // Guard: check retry count stored in job parameters
            JobParameters originalParams = latest.getJobParameters();
            long retryCount = getRetryCount(originalParams);

            if (retryCount >= maxRetries) {
                log.warn("Job [{}] instance [{}] has reached max retries ({}) — giving up",
                    jobName, instance.getInstanceId(), maxRetries);
                continue;
            }

            // Re-launch with the SAME original parameters + incremented retry count
            // Spring Batch resumes from the last FAILED step automatically
            JobParameters restartParams = buildRestartParams(originalParams, retryCount);

            log.info("Restarting job [{}] instance [{}] (retry {}/{})",
                jobName, instance.getInstanceId(), retryCount + 1, maxRetries);

            try {
                JobExecution newExecution = jobLauncher.run(job, restartParams);
                log.info("Job [{}] restarted successfully. New execution id: {}, status: {}",
                    jobName, newExecution.getId(), newExecution.getStatus());

            } catch (JobInstanceAlreadyCompleteException e) {
                log.info("Job [{}] instance [{}] already completed — skipping",
                    jobName, instance.getInstanceId());

            } catch (JobExecutionAlreadyRunningException e) {
                log.warn("Job [{}] instance [{}] is already running (race condition caught) — skipping",
                    jobName, instance.getInstanceId());

            } catch (Exception e) {
                log.error("Failed to restart job [{}] instance [{}]: {}",
                    jobName, instance.getInstanceId(), e.getMessage(), e);
            }
        }
    }

    /**
     * Reads the retry counter embedded in job parameters.
     * Returns 0 if no counter exists (first restart attempt).
     */
    private long getRetryCount(JobParameters params) {
        Long count = params.getLong("_retryCount");
        return count != null ? count : 0L;
    }

    /**
     * Builds restart parameters by carrying over ALL original parameters
     * and incrementing (or adding) the _retryCount marker.
     *
     * Preserving original parameters is critical — Spring Batch uses them
     * to locate the correct JobInstance in the repository and resume
     * from the failed step rather than starting fresh.
     */
    private JobParameters buildRestartParams(JobParameters original, long currentRetry) {
        JobParametersBuilder builder = new JobParametersBuilder(original);
        builder.addLong("_retryCount", currentRetry + 1);
        return builder.toJobParameters();
    }

}
