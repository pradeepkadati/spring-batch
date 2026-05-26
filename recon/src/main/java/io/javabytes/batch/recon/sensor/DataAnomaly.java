package io.javabytes.batch.recon.sensor;

import lombok.Getter;

@Getter
public class DataAnomaly {

    // Represented as 'MM-dd-yyyy'
    private String date;
    private AnomalyType type;
    private double value;

    public DataAnomaly(String date, AnomalyType type, double value) {
        this.date = date;
        this.type = type;
        this.value = value;
    }
}
