package io.javabytes.batch.s3sftp.launch;

import io.javabytes.batch.s3sftp.config.S3SftpProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Polls the inbound S3 prefix and launches one job execution per file found. Uses each object's
 * ETag as "sourceVersion" so that if the file is overwritten with new content before it's picked
 * up, it gets a fresh JobInstance rather than being mistaken for an already-completed run.
 *
 * JobInstanceAlreadyCompleteException and JobExecutionAlreadyRunningException are expected,
 * routine outcomes here (the same file listed again before archival, or two poll cycles
 * overlapping) - they are logged at debug level and not treated as errors.
 *
 * Disabled by default; enable with batch.s3sftp.poller.enabled=true once endpoint/credentials
 * are configured, or replace this with an S3 event notification -> SQS -> listener trigger for
 * near-real-time pickup instead of polling.
 */
@Component
@EnableConfigurationProperties(S3SftpProperties.class)
@ConditionalOnProperty(prefix = "batch.s3sftp.poller", name = "enabled", havingValue = "true")
public class S3InboundPoller {

    private static final Logger log = LoggerFactory.getLogger(S3InboundPoller.class);

    private final S3Client s3Client;
    private final JobLauncher jobLauncher;
    private final Job enrichAndDeliverJob;
    private final S3SftpProperties properties;

    public S3InboundPoller(S3Client s3Client, JobLauncher jobLauncher, Job enrichAndDeliverJob,
                            S3SftpProperties properties) {
        this.s3Client = s3Client;
        this.jobLauncher = jobLauncher;
        this.enrichAndDeliverJob = enrichAndDeliverJob;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${batch.s3sftp.poller.fixed-delay-millis:60000}")
    public void pollAndLaunch() {
        String bucket = properties.getS3().getBucket();
        String prefix = properties.getS3().getInboundPrefix();

        ListObjectsV2Response response = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket).prefix(prefix).build());

        for (S3Object object : response.contents()) {
            if (object.key().equals(prefix)) {
                continue; // the "directory marker" object itself
            }
            launch(object);
        }
    }

    private void launch(S3Object object) {
        var jobParameters = new JobParametersBuilder()
                .addString("sourceKey", object.key())
                .addString("sourceVersion", object.eTag())
                .toJobParameters();

        try {
            jobLauncher.run(enrichAndDeliverJob, jobParameters);
        } catch (JobInstanceAlreadyCompleteException e) {
            log.debug("Already processed s3://{}/{} (etag {}), skipping",
                    properties.getS3().getBucket(), object.key(), object.eTag());
        } catch (JobExecutionAlreadyRunningException e) {
            log.debug("Job already running for {}, skipping this poll cycle", object.key());
        } catch (JobRestartException e) {
            log.error("Job for {} is not restartable in its current state - needs manual intervention",
                    object.key(), e);
        } catch (Exception e) {
            log.error("Failed to launch job for s3://{}/{}", properties.getS3().getBucket(), object.key(), e);
        }
    }
}
