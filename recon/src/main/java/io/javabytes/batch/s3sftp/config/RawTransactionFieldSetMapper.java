package io.javabytes.batch.s3sftp.config;

import io.javabytes.batch.s3sftp.model.RawTransaction;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Deliberately throws on a malformed line rather than trying to coerce it, so bad rows surface
 * as a FlatFileParseException that the enrichStep's skip policy handles - not as a silent
 * default value like 0 or null that would corrupt downstream numbers.
 */
public class RawTransactionFieldSetMapper implements FieldSetMapper<RawTransaction> {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;

    @Override
    public RawTransaction mapFieldSet(FieldSet fieldSet) throws BindException {
        RawTransaction transaction = new RawTransaction();
        transaction.setTransactionId(fieldSet.readString("transactionId"));
        transaction.setAccountId(fieldSet.readString("accountId"));
        transaction.setAmount(new BigDecimal(fieldSet.readString("amount")));
        transaction.setCurrency(fieldSet.readString("currency"));
        transaction.setTransactionDate(LocalDate.parse(fieldSet.readString("transactionDate"), DATE_FORMAT));
        return transaction;
    }
}
