package io.javabytes.batch.recon.sensor;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.batch.item.file.LineMapper;

public class SensorDataTextMapper implements LineMapper<DailySensorData> {

    @Override
    public DailySensorData mapLine(String line, int lineNumber) throws Exception {
        String[] data = line.split(":");
        String date = data[0];
        String[] measurementTxt = data[1].split(",");
        List<Double> measurements = Arrays.stream(measurementTxt).map(Double::parseDouble).collect(Collectors.toList());
        return new DailySensorData(date, measurements);
    }

}
