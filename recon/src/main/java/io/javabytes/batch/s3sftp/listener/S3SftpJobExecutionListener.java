package io.javabytes.batch.s3sftp.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.stream.Collectors;

/**
 * Central failure-handling hook for the job.
 *
 * On failure: moves the source object to the "failed/" S3 prefix with a sidecar error report so
 * a human or a downstream alert can act on it without digging through batch logs, and leaves the
 * local staging directory in place for troubleshooting (unless configured to clean it up).
 *
 * On success: the S3ArchiveTasklet has already moved the source object, so this just cleans up
 * local staging files - there is nothing left worth keeping on disk.
 */
public class S3SftpJobExecutionListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(S3SftpJobExecutionListener.class);

    private final S3Client s3Client;
    private final String bucket;
    private final String sourceKey;
    private final String failedKey;
    private final Path stagingDir;
    private final boolean keepFilesOnFailure;

    public S3SftpJobExecutionListener(S3Client s3Client, String bucket, String sourceKey,
                                       String failedKey, Path stagingDir, boolean keepFilesOnFailure) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.sourceKey = sourceKey;
        this.failedKey = failedKey;
        this.stagingDir = stagingDir;
        this.keepFilesOnFailure = keepFilesOnFailure;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("Job {} completed successfully, cleaning up staging directory {}",
                    jobExecution.getJobId(), stagingDir);
            deleteRecursively(stagingDir);
            return;
        }

        log.error("Job {} finished with status {}, moving source to failed prefix",
                jobExecution.getJobId(), jobExecution.getStatus());
        try {
            moveSourceToFailed(jobExecution);
        } catch (Exception e) {
            // Best-effort: don't let archival failure mask the real job failure.
            log.error("Failed to move source object s3://{}/{} to failed prefix", bucket, sourceKey, e);
        }

        if (!keepFilesOnFailure) {
            deleteRecursively(stagingDir);
        } else {
            log.warn("Leaving staging directory {} in place for troubleshooting", stagingDir);
        }
    }

    private void moveSourceToFailed(JobExecution jobExecution) {
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
                software.amazon.awssdk.core.sync.RequestBody.fromString(
                        Instant.now() + System.lineSeparator() + errorReport, StandardCharsets.UTF_8));
    }

    private void deleteRecursively(Path dir) {
        if (!Files.exists(dir)) return;
        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path directory, IOException exc) throws IOException {
                    Files.delete(directory);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to clean up staging directory " + dir, e);
        }
    }
}
