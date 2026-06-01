package io.javabytes.batch.recon.bank;

import java.math.BigDecimal;

public class BankTransaction {

    private final long id;
    private final int month;
    private final int day;
    private final int hour;
    private final int minute;
    private final BigDecimal amount;
    private final String merchant;

    public BankTransaction(long id, int month, int day, int hour, int minute, BigDecimal amount, String merchant) {
        this.id = id;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.minute = minute;
        this.amount = amount;
        this.merchant = merchant;
    }

    public long getId() {
        return id;
    }

    public int getMonth() {
        return month;
    }

    public int getDay() {
        return day;
    }

    public int getHour() {
        return hour;
    }

    public int getMinute() {
        return minute;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getMerchant() {
        return merchant;
    }
}
