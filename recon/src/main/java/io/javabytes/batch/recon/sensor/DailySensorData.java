package io.javabytes.batch.recon.sensor;

import java.util.List;

public class DailySensorData {

    private final String date;
    private final List<Double> measurements;

    public DailySensorData(String date,List<Double> measurements){
        this.date = date;
        this.measurements = measurements;
    }

    public String getDate() {
        return date;
    }

    public List<Double> getMeasurements() {
        return measurements;
    }

}
