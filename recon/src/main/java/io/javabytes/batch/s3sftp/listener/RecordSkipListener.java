package io.javabytes.batch.s3sftp.listener;

import io.javabytes.batch.s3sftp.model.EnrichedTransaction;
import io.javabytes.batch.s3sftp.model.RawTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

/**
 * Writes every skipped record (bad line, failed validation, exhausted retries) to a local
 * rejects file for audit/reprocessing, instead of letting them silently disappear. This file
 * stays on disk even after the job succeeds so someone can review what was dropped.
 */
public class RecordSkipListener implements SkipListener<RawTransaction, EnrichedTransaction> {

    private static final Logger log = LoggerFactory.getLogger(RecordSkipListener.class);

    private final Path rejectsFile;

    public RecordSkipListener(Path rejectsFile) {
        this.rejectsFile = rejectsFile;
    }

    @Override
    public void onSkipInRead(Throwable t) {
        appendLine("READ_SKIP | " + t.getMessage());
        log.warn("Skipped unparsable line: {}", t.getMessage());
    }

    @Override
    public void onSkipInProcess(RawTransaction item, Throwable t) {
        appendLine("PROCESS_SKIP | " + item + " | " + t.getMessage());
        log.warn("Skipped record {} during enrichment: {}", item.getTransactionId(), t.getMessage());
    }

    @Override
    public void onSkipInWrite(EnrichedTransaction item, Throwable t) {
        appendLine("WRITE_SKIP | " + item.getTransactionId() + " | " + t.getMessage());
        log.warn("Skipped record {} during write: {}", item.getTransactionId(), t.getMessage());
    }

    private void appendLine(String line) {
        try {
            Files.createDirectories(rejectsFile.getParent());
            Files.writeString(rejectsFile, Instant.now() + " | " + line + System.lineSeparator(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("Failed to write to rejects file {}", rejectsFile, e);
        }
    }
}
