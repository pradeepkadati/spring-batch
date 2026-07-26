package io.javabytes.batch.s3sftp.tasklet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.retry.support.RetryTemplate;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Downloads the source object from the NetApp S3 bucket to a local staging file. Downloading up
 * front (rather than streaming S3 directly into the reader) lets the enrichment step use a plain
 * FlatFileItemReader against a local File, which supports Spring Batch's normal restart/seek
 * behaviour - something a one-shot S3 InputStream cannot do.
 */
public class S3DownloadTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(S3DownloadTasklet.class);

    private final S3Client s3Client;
    private final RetryTemplate retryTemplate;
    private final String bucket;
    private final String sourceKey;
    private final Path localTargetFile;

    public S3DownloadTasklet(S3Client s3Client, RetryTemplate retryTemplate,
                              String bucket, String sourceKey, Path localTargetFile) {
        this.s3Client = s3Client;
        this.retryTemplate = retryTemplate;
        this.bucket = bucket;
        this.sourceKey = sourceKey;
        this.localTargetFile = localTargetFile;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        // Idempotent restart: if a previous attempt already downloaded the file successfully,
        // don't hit S3 again.
        if (Files.exists(localTargetFile) && Files.size(localTargetFile) > 0) {
            log.info("Staging file {} already present, skipping re-download", localTargetFile);
            return RepeatStatus.FINISHED;
        }

        Files.createDirectories(localTargetFile.getParent());

        try {
            retryTemplate.execute(context -> {
                log.info("Downloading s3://{}/{} to {} (attempt {})",
                        bucket, sourceKey, localTargetFile, context.getRetryCount() + 1);
                s3Client.getObject(
                        GetObjectRequest.builder().bucket(bucket).key(sourceKey).build(),
                        localTargetFile);
                return null;
            });
        } catch (NoSuchKeyException e) {
            // Permanent - retrying won't make the object appear. Fail fast, don't burn retries.
            throw new IllegalStateException("Source object not found: s3://" + bucket + "/" + sourceKey, e);
        }

        contribution.getStepExecution().getExecutionContext().putString("downloadedFile", localTargetFile.toString());
        return RepeatStatus.FINISHED;
    }
}
