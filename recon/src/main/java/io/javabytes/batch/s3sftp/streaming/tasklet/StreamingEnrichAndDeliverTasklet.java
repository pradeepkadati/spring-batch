package io.javabytes.batch.s3sftp.streaming.tasklet;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import io.javabytes.batch.s3sftp.config.RawTransactionFieldSetMapper;
import io.javabytes.batch.s3sftp.config.S3SftpProperties;
import io.javabytes.batch.s3sftp.exception.RecordValidationException;
import io.javabytes.batch.s3sftp.exception.TransientEnrichmentException;
import io.javabytes.batch.s3sftp.model.EnrichedTransaction;
import io.javabytes.batch.s3sftp.model.RawTransaction;
import io.javabytes.batch.s3sftp.processor.EnrichmentItemProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.retry.support.RetryTemplate;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

/**
 * Streams a source object straight from NetApp S3 to the SFTP target, enriching it line by line,
 * without ever writing anything to local disk. Built for OpenShift pods that have no writable
 * volume (or a tiny ephemeral one) - memory use is bounded by one line at a time, not by file
 * size, so it scales to large files the same way regardless of container storage limits.
 *
 * This trades away the fine-grained, per-record restart of the disk-based pipeline
 * ({@code io.javabytes.batch.s3sftp} - unchanged, still the right choice when a writable volume
 * is available) for a coarser one: since neither the S3 read stream nor the SFTP write stream is
 * seekable/resumable, any infrastructure failure mid-transfer aborts and retries the *entire*
 * transfer from byte 0, via the outer RetryTemplate. The SFTP side writes to a ".part" name and
 * only renames it to the final name after a fully successful pass, and any partial ".part" file
 * from a failed attempt is removed before the next attempt starts - so a partner watching the
 * remote directory never sees a half-written file, and retries are always clean re-runs.
 *
 * Per-record data problems (bad rows, transient enrichment lookups) are still handled at record
 * granularity, same policy as the disk-based pipeline: validation failures are skipped (up to a
 * limit) and reported, transient enrichment failures are retried a few times before being
 * counted as a skip.
 */
public class StreamingEnrichAndDeliverTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(StreamingEnrichAndDeliverTasklet.class);
    private static final String HEADER =
            "transactionId,accountId,amount,currency,transactionDate,feeAmount,netAmount,riskFlag,enrichedAt";
    private static final String[] OUTPUT_FIELDS = {
            "transactionId", "accountId", "amount", "currency", "transactionDate",
            "feeAmount", "netAmount", "riskFlag", "enrichedAt"
    };

    /** Thrown when the skip limit is exceeded - a data-quality problem, never worth retrying. */
    static class SkipLimitExceededException extends RuntimeException {
        SkipLimitExceededException(String message) { super(message); }
    }

    private final S3Client s3Client;
    private final RetryTemplate infrastructureRetryTemplate;
    private final S3SftpProperties properties;
    private final String bucket;
    private final String sourceKey;
    private final String sourceVersion;
    private final String remoteFileName;
    private final EnrichmentItemProcessor processor = new EnrichmentItemProcessor();
    private final DelimitedLineTokenizer tokenizer = buildTokenizer();
    private final RawTransactionFieldSetMapper fieldSetMapper = new RawTransactionFieldSetMapper();
    private final DelimitedLineAggregator<EnrichedTransaction> lineAggregator = buildLineAggregator();

    public StreamingEnrichAndDeliverTasklet(S3Client s3Client, RetryTemplate infrastructureRetryTemplate,
                                             S3SftpProperties properties, String bucket, String sourceKey,
                                             String sourceVersion, String remoteFileName) {
        this.s3Client = s3Client;
        this.infrastructureRetryTemplate = infrastructureRetryTemplate;
        this.properties = properties;
        this.bucket = bucket;
        this.sourceKey = sourceKey;
        this.sourceVersion = sourceVersion;
        this.remoteFileName = remoteFileName;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        try {
            RunStats stats = infrastructureRetryTemplate.execute(context -> {
                log.info("Streaming s3://{}/{} -> sftp {} (attempt {})",
                        bucket, sourceKey, remoteFileName, context.getRetryCount() + 1);
                return runOnce();
            });
            log.info("Streaming transfer complete for {}: {} written, {} skipped",
                    sourceKey, stats.written, stats.skipped);
            contribution.incrementWriteCount(stats.written);
            if (stats.skipped > 0) {
                uploadRejectsReport(stats.rejectLines);
            }
        } catch (NoSuchKeyException e) {
            throw new IllegalStateException("Source object not found: s3://" + bucket + "/" + sourceKey, e);
        }
        return RepeatStatus.FINISHED;
    }

    private static class RunStats {
        int written;
        int skipped;
        final List<String> rejectLines = new ArrayList<>();
    }

    private RunStats runOnce() throws Exception {
        S3SftpProperties.Sftp sftpProps = properties.getSftp();
        String partName = remoteFileName + ".part";

        Session session = null;
        ChannelSftp channel = null;
        try {
            session = openSftpSession(sftpProps);
            channel = openSftpChannel(session);
            channel.cd(sftpProps.getRemoteDir());

            RunStats stats = streamTransfer(channel, partName);

            removeIfExists(channel, remoteFileName);
            channel.rename(partName, remoteFileName);
            return stats;
        } catch (Exception e) {
            if (channel != null) {
                removeIfExists(channel, partName);
            }
            throw e;
        } finally {
            if (channel != null) channel.disconnect();
            if (session != null) session.disconnect();
        }
    }

    private RunStats streamTransfer(ChannelSftp channel, String partName) throws Exception {
        RunStats stats = new RunStats();
        int skipLimit = properties.getRetry().getSkipLimit();
        int processRetryLimit = properties.getRetry().getProcessRetryLimit();

        GetObjectRequest getRequest = GetObjectRequest.builder().bucket(bucket).key(sourceKey).build();

        try (ResponseInputStream<GetObjectResponse> s3In = s3Client.getObject(getRequest);
             BufferedReader reader = new BufferedReader(new InputStreamReader(s3In, StandardCharsets.UTF_8))) {

            OutputStream sftpOut = channel.put(partName);
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(new BufferedOutputStream(sftpOut), StandardCharsets.UTF_8))) {

                writer.write(HEADER);
                writer.newLine();

                reader.readLine(); // skip source header line
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank()) {
                        continue;
                    }
                    boolean written = processLine(line, writer, processRetryLimit, stats);
                    if (written) {
                        stats.written++;
                    } else {
                        stats.skipped++;
                        if (stats.skipped > skipLimit) {
                            throw new SkipLimitExceededException(
                                    "Skip limit (" + skipLimit + ") exceeded while processing " + sourceKey);
                        }
                    }
                }
            }
        }
        return stats;
    }

    private boolean processLine(String line, BufferedWriter writer, int processRetryLimit, RunStats stats) throws IOException {
        RawTransaction raw;
        try {
            FieldSet fieldSet = tokenizer.tokenize(line);
            raw = fieldSetMapper.mapFieldSet(fieldSet);
        } catch (Exception e) {
            recordReject(stats, "PARSE_SKIP", line, e);
            return false;
        }

        EnrichedTransaction enriched = null;
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                enriched = processor.process(raw);
                break;
            } catch (RecordValidationException e) {
                recordReject(stats, "VALIDATION_SKIP", raw.toString(), e);
                return false;
            } catch (TransientEnrichmentException e) {
                if (attempt > processRetryLimit) {
                    recordReject(stats, "RETRY_EXHAUSTED_SKIP", raw.toString(), e);
                    return false;
                }
                log.warn("Transient enrichment failure for {} (attempt {}), retrying: {}",
                        raw.getTransactionId(), attempt, e.getMessage());
            }
        }

        writer.write(lineAggregator.aggregate(enriched));
        writer.newLine();
        return true;
    }

    private void recordReject(RunStats stats, String reason, String detail, Exception cause) {
        String rejectLine = reason + " | " + detail + " | " + cause.getMessage();
        log.warn(rejectLine);
        stats.rejectLines.add(rejectLine);
    }

    private void uploadRejectsReport(List<String> rejectLines) {
        try {
            String rejectsKey = "rejects/" + baseName(sourceKey) + "-" + sourceVersion + ".log";
            s3Client.putObject(
                    PutObjectRequest.builder().bucket(bucket).key(rejectsKey).build(),
                    RequestBody.fromString(String.join(System.lineSeparator(), rejectLines), StandardCharsets.UTF_8));
            log.info("Uploaded rejects report to s3://{}/{}", bucket, rejectsKey);
        } catch (Exception e) {
            // Best-effort - losing the rejects report must never fail an otherwise-successful job.
            log.error("Failed to upload rejects report for {}", sourceKey, e);
        }
    }

    private Session openSftpSession(S3SftpProperties.Sftp sftpProps) throws Exception {
        JSch jsch = new JSch();
        if (sftpProps.getPrivateKeyPath() != null) {
            jsch.addIdentity(sftpProps.getPrivateKeyPath());
        }
        if (sftpProps.getKnownHostsPath() != null) {
            jsch.setKnownHosts(sftpProps.getKnownHostsPath());
        }
        Session session = jsch.getSession(sftpProps.getUser(), sftpProps.getHost(), sftpProps.getPort());
        if (sftpProps.getPassword() != null) {
            session.setPassword(sftpProps.getPassword());
        }
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", sftpProps.isStrictHostKeyChecking() ? "yes" : "no");
        session.setConfig(config);
        session.setTimeout(sftpProps.getConnectTimeoutMillis());
        session.connect();
        return session;
    }

    private ChannelSftp openSftpChannel(Session session) throws Exception {
        ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");
        channel.connect();
        return channel;
    }

    @SuppressWarnings("unchecked")
    private void removeIfExists(ChannelSftp channel, String fileName) {
        try {
            Vector<ChannelSftp.LsEntry> entries = channel.ls(fileName);
            if (entries != null && !entries.isEmpty()) {
                channel.rm(fileName);
            }
        } catch (Exception e) {
            // ls throws if the file doesn't exist - the expected, common case.
        }
    }

    private static String baseName(String key) {
        int idx = key.lastIndexOf('/');
        return idx >= 0 ? key.substring(idx + 1) : key;
    }

    private static DelimitedLineTokenizer buildTokenizer() {
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("transactionId", "accountId", "amount", "currency", "transactionDate");
        return tokenizer;
    }

    private static DelimitedLineAggregator<EnrichedTransaction> buildLineAggregator() {
        BeanWrapperFieldExtractor<EnrichedTransaction> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(OUTPUT_FIELDS);
        DelimitedLineAggregator<EnrichedTransaction> aggregator = new DelimitedLineAggregator<>();
        aggregator.setFieldExtractor(fieldExtractor);
        return aggregator;
    }
}
