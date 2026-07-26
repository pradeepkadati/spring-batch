package io.javabytes.batch.s3sftp.streaming.config;

import io.javabytes.batch.s3sftp.config.S3SftpProperties;
import io.javabytes.batch.s3sftp.streaming.listener.StreamingJobExecutionListener;
import io.javabytes.batch.s3sftp.streaming.tasklet.StreamingEnrichAndDeliverTasklet;
import io.javabytes.batch.s3sftp.tasklet.S3ArchiveTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * OpenShift-friendly variant of the pipeline: no writable local filesystem is used anywhere.
 *
 * 1. streamStep  - tasklet, S3 GetObject stream -> enrich in memory, line by line -> SFTP
 *                  OutputStream, with no intermediate file on either end
 * 2. archiveStep - tasklet, reused as-is from io.javabytes.batch.s3sftp.tasklet.S3ArchiveTasklet,
 *                  since it only calls the S3 API and never touched local disk to begin with
 *
 * Same JobInstance/idempotency strategy as the disk-based pipeline: JobParameters are
 * (sourceKey, sourceVersion) with no RunIdIncrementer, so a restart after failure re-runs the
 * failed step(s) against the same source object rather than creating a duplicate execution.
 * The trade-off documented on StreamingEnrichAndDeliverTasklet is that a restart of streamStep
 * itself always redoes the full transfer, since neither the S3 read nor the SFTP write side is
 * resumable mid-stream - there is no local file to resume from.
 */
@Configuration
@EnableConfigurationProperties(S3SftpProperties.class)
public class StreamingS3SftpBatchConfig {

    @Bean
    public Job streamingEnrichAndDeliverJob(JobRepository jobRepository,
                                             Step streamStep, Step streamingArchiveStep,
                                             StreamingJobExecutionListener streamingJobExecutionListener) {
        return new JobBuilder("streamingEnrichAndDeliverJob", jobRepository)
                .listener(streamingJobExecutionListener)
                .start(streamStep)
                .next(streamingArchiveStep)
                .build();
    }

    @Bean
    public Step streamStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                            Tasklet streamingEnrichAndDeliverTasklet) {
        return new StepBuilder("streamStep", jobRepository)
                .tasklet(streamingEnrichAndDeliverTasklet, transactionManager)
                .build();
    }

    // Named distinctly from the disk-based pipeline's "archiveStep" bean (both configs are
    // loaded into the same application context) even though the underlying tasklet is reused.
    @Bean
    public Step streamingArchiveStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                      Tasklet streamingS3ArchiveTasklet) {
        return new StepBuilder("streamingArchiveStep", jobRepository)
                .tasklet(streamingS3ArchiveTasklet, transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet streamingEnrichAndDeliverTasklet(S3Client s3Client, RetryTemplate infrastructureRetryTemplate,
                                                      S3SftpProperties properties,
                                                      @Value("#{jobParameters['sourceKey']}") String sourceKey,
                                                      @Value("#{jobParameters['sourceVersion']}") String sourceVersion) {
        String remoteFileName = baseName(sourceKey) + "-enriched.csv";
        return new StreamingEnrichAndDeliverTasklet(s3Client, infrastructureRetryTemplate, properties,
                properties.getS3().getBucket(), sourceKey, sourceVersion, remoteFileName);
    }

    @Bean
    @StepScope
    public Tasklet streamingS3ArchiveTasklet(S3Client s3Client, S3SftpProperties properties,
                                              @Value("#{jobParameters['sourceKey']}") String sourceKey) {
        String processedKey = properties.getS3().getProcessedPrefix() + baseName(sourceKey);
        return new S3ArchiveTasklet(s3Client, properties.getS3().getBucket(), sourceKey, processedKey);
    }

    @Bean
    @JobScope
    public StreamingJobExecutionListener streamingJobExecutionListener(
            S3Client s3Client, S3SftpProperties properties,
            @Value("#{jobParameters['sourceKey']}") String sourceKey) {
        String failedKey = properties.getS3().getFailedPrefix() + baseName(sourceKey);
        return new StreamingJobExecutionListener(s3Client, properties.getS3().getBucket(), sourceKey, failedKey);
    }

    private static String baseName(String key) {
        int idx = key.lastIndexOf('/');
        return idx >= 0 ? key.substring(idx + 1) : key;
    }
}
