package io.javabytes.batch.s3sftp.exception;

/**
 * Thrown when enrichment fails for a reason that is likely to succeed on retry - e.g. a
 * downstream reference-data/lookup call timing out. The chunk step retries records that throw
 * this a bounded number of times before giving up and treating it as a skip.
 */
public class TransientEnrichmentException extends RuntimeException {

    public TransientEnrichmentException(String message, Throwable cause) {
        super(message, cause);
    }
}
