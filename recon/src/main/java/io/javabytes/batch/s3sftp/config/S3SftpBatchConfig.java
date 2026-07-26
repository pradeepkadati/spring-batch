package io.javabytes.batch.s3sftp.config;

import io.javabytes.batch.s3sftp.exception.RecordValidationException;
import io.javabytes.batch.s3sftp.exception.TransientEnrichmentException;
import io.javabytes.batch.s3sftp.listener.RecordSkipListener;
import io.javabytes.batch.s3sftp.listener.S3SftpJobExecutionListener;
import io.javabytes.batch.s3sftp.model.EnrichedTransaction;
import io.javabytes.batch.s3sftp.model.RawTransaction;
import io.javabytes.batch.s3sftp.processor.EnrichmentItemProcessor;
import io.javabytes.batch.s3sftp.tasklet.S3ArchiveTasklet;
import io.javabytes.batch.s3sftp.tasklet.S3DownloadTasklet;
import io.javabytes.batch.s3sftp.tasklet.SftpUploadTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Wires the four-step pipeline for one source file:
 *
 * 1. downloadStep  - tasklet, pulls the S3 object to local disk
 * 2. enrichStep    - chunk step, reads/enriches/writes locally, with skip+retry fault tolerance
 * 3. uploadStep    - tasklet, pushes the enriched file to the SFTP target
 * 4. archiveStep   - tasklet, moves the S3 source object to processed/
 *
 * Job parameters "sourceKey" and "sourceVersion" (e.g. the S3 object's ETag or last-modified
 * timestamp) identify the JobInstance. No RunIdIncrementer is used deliberately: if the job
 * fails partway through, relaunching it with the *same* parameters resumes at the failed step
 * instead of redoing completed ones, and Spring Batch will refuse to rerun a JobInstance that
 * already COMPLETED - which is exactly the idempotency guarantee you want for file processing.
 *
 * Every component that needs a job parameter (readers, writers, tasklets, the job listener) is
 * declared @StepScope/@JobScope so the "#{jobParameters[...]}" SpEL is resolved lazily against
 * the running execution, rather than at application-context startup when no parameters exist.
 * The Job/Step definition beans themselves stay plain singletons and just wire those proxies.
 */
@Configuration
@EnableConfigurationProperties(S3SftpProperties.class)
public class S3SftpBatchConfig {

    @Bean
    public Job enrichAndDeliverJob(JobRepository jobRepository,
                                    Step downloadStep, Step enrichStep, Step uploadStep, Step archiveStep,
                                    S3SftpJobExecutionListener jobExecutionListener) {
        return new JobBuilder("enrichAndDeliverJob", jobRepository)
                .listener(jobExecutionListener)
                .start(downloadStep)
                .next(enrichStep)
                .next(uploadStep)
                .next(archiveStep)
                .build();
    }

    @Bean
    public Step downloadStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                              Tasklet s3DownloadTasklet) {
        return new StepBuilder("downloadStep", jobRepository)
                .tasklet(s3DownloadTasklet, transactionManager)
                .build();
    }

    @Bean
    public Step enrichStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                            S3SftpProperties properties,
                            FlatFileItemReader<RawTransaction> rawTransactionReader,
                            FlatFileItemWriter<EnrichedTransaction> enrichedTransactionWriter,
                            RecordSkipListener recordSkipListener) {
        S3SftpProperties.Retry retryProps = properties.getRetry();

        return new StepBuilder("enrichStep", jobRepository)
                .<RawTransaction, EnrichedTransaction>chunk(retryProps.getChunkSize(), transactionManager)
                .reader(rawTransactionReader)
                .processor(new EnrichmentItemProcessor())
                .writer(enrichedTransactionWriter)
                .faultTolerant()
                // Permanent, bad-data problems: skip the record, keep the job going.
                .skip(RecordValidationException.class)
                .skip(FlatFileParseException.class)
                .skipLimit(retryProps.getSkipLimit())
                // Transient downstream problems: retry the record before giving up on it.
                .retry(TransientEnrichmentException.class)
                .retryLimit(retryProps.getProcessRetryLimit())
                .listener(recordSkipListener)
                .build();
    }

    @Bean
    public Step uploadStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                            Tasklet sftpUploadTasklet) {
        return new StepBuilder("uploadStep", jobRepository)
                .tasklet(sftpUploadTasklet, transactionManager)
                .build();
    }

    @Bean
    public Step archiveStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                             Tasklet s3ArchiveTasklet) {
        return new StepBuilder("archiveStep", jobRepository)
                .tasklet(s3ArchiveTasklet, transactionManager)
                .build();
    }

    // ---- Step-scoped / job-scoped beans: job parameters resolve lazily here ----

    @Bean
    @StepScope
    public Tasklet s3DownloadTasklet(S3Client s3Client, RetryTemplate retryTemplate, S3SftpProperties properties,
                                      StagingPathResolver paths,
                                      @Value("#{jobParameters['sourceKey']}") String sourceKey,
                                      @Value("#{jobParameters['sourceVersion']}") String sourceVersion) {
        return new S3DownloadTasklet(s3Client, retryTemplate, properties.getS3().getBucket(),
                sourceKey, paths.rawInputFile(sourceKey, sourceVersion));
    }

    @Bean
    @StepScope
    public Tasklet sftpUploadTasklet(S3SftpProperties properties, RetryTemplate retryTemplate,
                                      StagingPathResolver paths,
                                      @Value("#{jobParameters['sourceKey']}") String sourceKey,
                                      @Value("#{jobParameters['sourceVersion']}") String sourceVersion) {
        String remoteFileName = deriveFileName(sourceKey) + "-enriched.csv";
        return new SftpUploadTasklet(properties.getSftp(), retryTemplate,
                paths.enrichedOutputFile(sourceKey, sourceVersion), remoteFileName);
    }

    @Bean
    @StepScope
    public Tasklet s3ArchiveTasklet(S3Client s3Client, S3SftpProperties properties,
                                     @Value("#{jobParameters['sourceKey']}") String sourceKey) {
        String processedKey = properties.getS3().getProcessedPrefix() + deriveFileName(sourceKey);
        return new S3ArchiveTasklet(s3Client, properties.getS3().getBucket(), sourceKey, processedKey);
    }

    @Bean
    @JobScope
    public S3SftpJobExecutionListener jobExecutionListener(S3Client s3Client, S3SftpProperties properties,
                                                             StagingPathResolver paths,
                                                             @Value("#{jobParameters['sourceKey']}") String sourceKey,
                                                             @Value("#{jobParameters['sourceVersion']}") String sourceVersion) {
        String failedKey = properties.getS3().getFailedPrefix() + deriveFileName(sourceKey);
        return new S3SftpJobExecutionListener(s3Client, properties.getS3().getBucket(), sourceKey, failedKey,
                paths.rawInputFile(sourceKey, sourceVersion).getParent(),
                properties.getLocal().isKeepFilesOnFailure());
    }

    @Bean
    @StepScope
    public RecordSkipListener recordSkipListener(StagingPathResolver paths,
                                                  @Value("#{jobParameters['sourceKey']}") String sourceKey,
                                                  @Value("#{jobParameters['sourceVersion']}") String sourceVersion) {
        return new RecordSkipListener(paths.rejectsFile(sourceKey, sourceVersion));
    }

    @Bean
    @StepScope
    public FlatFileItemReader<RawTransaction> rawTransactionReader(StagingPathResolver paths,
                                                                     @Value("#{jobParameters['sourceKey']}") String sourceKey,
                                                                     @Value("#{jobParameters['sourceVersion']}") String sourceVersion) {
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("transactionId", "accountId", "amount", "currency", "transactionDate");

        DefaultLineMapper<RawTransaction> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new RawTransactionFieldSetMapper());
        lineMapper.afterPropertiesSet();

        FlatFileItemReader<RawTransaction> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource(paths.rawInputFile(sourceKey, sourceVersion)));
        reader.setLineMapper(lineMapper);
        reader.setLinesToSkip(1); // header
        // saveState defaults to true: on restart, the reader resumes from the last committed
        // chunk boundary instead of re-reading the whole file from line 1.
        return reader;
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<EnrichedTransaction> enrichedTransactionWriter(StagingPathResolver paths,
                                                                              @Value("#{jobParameters['sourceKey']}") String sourceKey,
                                                                              @Value("#{jobParameters['sourceVersion']}") String sourceVersion) {
        BeanWrapperFieldExtractor<EnrichedTransaction> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {
                "transactionId", "accountId", "amount", "currency", "transactionDate",
                "feeAmount", "netAmount", "riskFlag", "enrichedAt"
        });

        DelimitedLineAggregator<EnrichedTransaction> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setFieldExtractor(fieldExtractor);

        FlatFileItemWriter<EnrichedTransaction> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource(paths.enrichedOutputFile(sourceKey, sourceVersion)));
        writer.setLineAggregator(lineAggregator);
        writer.setHeaderCallback(w -> w.write(
                "transactionId,accountId,amount,currency,transactionDate,feeAmount,netAmount,riskFlag,enrichedAt"));
        // append=true so a restart resumes writing where the last failed attempt left off,
        // matching the reader's resumed position, instead of truncating already-written output.
        writer.setAppendAllowed(true);
        return writer;
    }

    private static String deriveFileName(String sourceKey) {
        int idx = sourceKey.lastIndexOf('/');
        return idx >= 0 ? sourceKey.substring(idx + 1) : sourceKey;
    }
}
