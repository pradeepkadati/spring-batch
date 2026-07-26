package io.javabytes.batch.s3sftp.exception;

/**
 * Permanent, data-quality problem with a single record (bad amount, unknown currency, etc).
 * Retrying will never fix this - the chunk step is configured to skip records that throw it
 * and log them to the rejects file instead of failing the whole job.
 */
public class RecordValidationException extends RuntimeException {

    public RecordValidationException(String message) {
        super(message);
    }
}
