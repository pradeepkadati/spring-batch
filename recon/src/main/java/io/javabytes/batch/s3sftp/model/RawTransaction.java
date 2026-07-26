package io.javabytes.batch.s3sftp.model;

import java.math.BigDecimal;
import java.time.LocalDate;

/** One line of the inbound file: transactionId,accountId,amount,currency,transactionDate */
public class RawTransaction {

    private String transactionId;
    private String accountId;
    private BigDecimal amount;
    private String currency;
    private LocalDate transactionDate;

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public String getAccountId() { return accountId; }
    public void setAccountId(String accountId) { this.accountId = accountId; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }
    public LocalDate getTransactionDate() { return transactionDate; }
    public void setTransactionDate(LocalDate transactionDate) { this.transactionDate = transactionDate; }

    @Override
    public String toString() {
        return "RawTransaction{transactionId=%s, accountId=%s, amount=%s, currency=%s, transactionDate=%s}"
                .formatted(transactionId, accountId, amount, currency, transactionDate);
    }
}
