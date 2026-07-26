package io.javabytes.batch.s3sftp.streaming.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.stream.Collectors;

/**
 * Equivalent of {@code io.javabytes.batch.s3sftp.listener.S3SftpJobExecutionListener} for the
 * diskless pipeline - there is no local staging directory to clean up here, since nothing was
 * ever written to disk. On failure it still moves the source object to the failed/ prefix with
 * an error sidecar, so a partially-streamed transfer is just as visible/actionable as before.
 */
public class StreamingJobExecutionListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(StreamingJobExecutionListener.class);

    private final S3Client s3Client;
    private final String bucket;
    private final String sourceKey;
    private final String failedKey;

    public StreamingJobExecutionListener(S3Client s3Client, String bucket, String sourceKey, String failedKey) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.sourceKey = sourceKey;
        this.failedKey = failedKey;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("Job {} completed successfully for s3://{}/{}", jobExecution.getJobId(), bucket, sourceKey);
            return;
        }

        log.error("Job {} finished with status {}, moving source to failed prefix",
                jobExecution.getJobId(), jobExecution.getStatus());
        try {
            String errorReport = jobExecution.getAllFailureExceptions().stream()
                    .map(Throwable::toString)
                    .collect(Collectors.joining(System.lineSeparator()));

            s3Client.copyObject(CopyObjectRequest.builder()
                    .sourceBucket(bucket).sourceKey(sourceKey)
                    .destinationBucket(bucket).destinationKey(failedKey)
                    .build());
            s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(sourceKey).build());
            s3Client.putObject(
                    PutObjectRequest.builder().bucket(bucket).key(failedKey + ".error.txt").build(),
                    RequestBody.fromString(Instant.now() + System.lineSeparator() + errorReport, StandardCharsets.UTF_8));
        } catch (Exception e) {
            // Best-effort: don't let archival failure mask the real job failure.
            log.error("Failed to move source object s3://{}/{} to failed prefix", bucket, sourceKey, e);
        }
    }
}
