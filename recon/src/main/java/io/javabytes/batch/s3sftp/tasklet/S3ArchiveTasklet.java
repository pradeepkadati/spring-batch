package io.javabytes.batch.s3sftp.tasklet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

/**
 * Runs only after the SFTP upload step has completed successfully. S3 has no native move, so
 * this copies the source object under the "processed/" prefix and deletes the original -
 * marking the file as done and preventing a poller from ever picking it up again.
 */
public class S3ArchiveTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(S3ArchiveTasklet.class);

    private final S3Client s3Client;
    private final String bucket;
    private final String sourceKey;
    private final String processedKey;

    public S3ArchiveTasklet(S3Client s3Client, String bucket, String sourceKey, String processedKey) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.sourceKey = sourceKey;
        this.processedKey = processedKey;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        s3Client.copyObject(CopyObjectRequest.builder()
                .sourceBucket(bucket).sourceKey(sourceKey)
                .destinationBucket(bucket).destinationKey(processedKey)
                .build());
        s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(sourceKey).build());
        log.info("Archived s3://{}/{} -> s3://{}/{}", bucket, sourceKey, bucket, processedKey);
        return RepeatStatus.FINISHED;
    }
}
