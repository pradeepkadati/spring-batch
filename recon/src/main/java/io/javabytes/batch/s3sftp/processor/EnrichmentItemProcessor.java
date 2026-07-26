package io.javabytes.batch.s3sftp.processor;

import io.javabytes.batch.s3sftp.exception.RecordValidationException;
import io.javabytes.batch.s3sftp.exception.TransientEnrichmentException;
import io.javabytes.batch.s3sftp.model.EnrichedTransaction;
import io.javabytes.batch.s3sftp.model.RawTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.SocketTimeoutException;
import java.time.Instant;
import java.util.Set;

/**
 * Validates each raw record, then enriches it with a fee/net amount and a risk flag.
 * Distinguishes permanent data problems (RecordValidationException -> skip) from transient
 * downstream failures (TransientEnrichmentException -> retry) so the step's fault-tolerance
 * config can react to each appropriately.
 */
public class EnrichmentItemProcessor implements ItemProcessor<RawTransaction, EnrichedTransaction> {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentItemProcessor.class);
    private static final Set<String> SUPPORTED_CURRENCIES = Set.of("USD", "EUR", "GBP", "JPY");
    private static final BigDecimal HIGH_VALUE_THRESHOLD = new BigDecimal("10000");

    @Override
    public EnrichedTransaction process(RawTransaction raw) {
        validate(raw);

        BigDecimal feeRate = lookupFeeRate(raw.getCurrency());
        BigDecimal feeAmount = raw.getAmount().multiply(feeRate).setScale(2, RoundingMode.HALF_UP);
        BigDecimal netAmount = raw.getAmount().subtract(feeAmount);

        EnrichedTransaction enriched = new EnrichedTransaction();
        enriched.setTransactionId(raw.getTransactionId());
        enriched.setAccountId(raw.getAccountId());
        enriched.setAmount(raw.getAmount());
        enriched.setCurrency(raw.getCurrency());
        enriched.setTransactionDate(raw.getTransactionDate());
        enriched.setFeeAmount(feeAmount);
        enriched.setNetAmount(netAmount);
        enriched.setRiskFlag(raw.getAmount().compareTo(HIGH_VALUE_THRESHOLD) > 0 ? "REVIEW" : "OK");
        enriched.setEnrichedAt(Instant.now());
        return enriched;
    }

    private void validate(RawTransaction raw) {
        if (raw.getAmount() == null || raw.getAmount().signum() <= 0) {
            throw new RecordValidationException(
                    "Invalid amount for transaction " + raw.getTransactionId() + ": " + raw.getAmount());
        }
        if (raw.getCurrency() == null || !SUPPORTED_CURRENCIES.contains(raw.getCurrency())) {
            throw new RecordValidationException(
                    "Unsupported currency for transaction " + raw.getTransactionId() + ": " + raw.getCurrency());
        }
    }

    /**
     * Stand-in for a real reference-data/rate lookup (REST call, cache, DB). Wrap the real
     * client call the same way: catch network/timeout exceptions specifically and rethrow as
     * TransientEnrichmentException so they go through the step's retry policy instead of
     * immediately failing/skipping the record.
     */
    private BigDecimal lookupFeeRate(String currency) {
        try {
            return switch (currency) {
                case "USD" -> new BigDecimal("0.010");
                case "EUR" -> new BigDecimal("0.012");
                case "GBP" -> new BigDecimal("0.015");
                case "JPY" -> new BigDecimal("0.008");
                default -> throw new RecordValidationException("No fee rate configured for " + currency);
            };
        } catch (RecordValidationException e) {
            throw e;
        } catch (RuntimeException e) {
            if (e.getCause() instanceof SocketTimeoutException) {
                log.warn("Transient timeout looking up fee rate for {}, will retry", currency);
                throw new TransientEnrichmentException("Timed out looking up fee rate for " + currency, e);
            }
            throw e;
        }
    }
}
