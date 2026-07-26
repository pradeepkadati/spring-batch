package io.javabytes.batch.s3sftp.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import java.nio.file.Path;

/**
 * Derives deterministic local file paths from job parameters instead of passing paths through
 * the Spring Batch ExecutionContext. Paths are keyed by (sourceKey, sourceVersion) - the job
 * parameters that stay identical across a restart of the same JobInstance - rather than by
 * JobExecution id, which changes on every retry attempt. That's what lets the download step
 * detect "already downloaded" and skip re-fetching from S3 on restart, and what lets the
 * upload step find the same enriched file a previous, failed attempt already produced.
 */
@Component
@EnableConfigurationProperties(S3SftpProperties.class)
public class StagingPathResolver {

    private final S3SftpProperties properties;

    public StagingPathResolver(S3SftpProperties properties) {
        this.properties = properties;
    }

    public Path rawInputFile(String sourceKey, String sourceVersion) {
        return jobDir(sourceKey, sourceVersion).resolve("raw-input.txt");
    }

    public Path enrichedOutputFile(String sourceKey, String sourceVersion) {
        return jobDir(sourceKey, sourceVersion).resolve("enriched-output.txt");
    }

    public Path rejectsFile(String sourceKey, String sourceVersion) {
        return jobDir(sourceKey, sourceVersion).resolve("rejects.log");
    }

    private Path jobDir(String sourceKey, String sourceVersion) {
        String sanitizedKey = sourceKey.replaceAll("[^a-zA-Z0-9._-]", "_");
        String sanitizedVersion = sourceVersion.replaceAll("[^a-zA-Z0-9._-]", "_");
        return properties.getLocal().getStagingDir()
                .resolve(sanitizedKey + "-" + sanitizedVersion);
    }
}
