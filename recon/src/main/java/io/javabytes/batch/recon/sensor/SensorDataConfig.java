package io.javabytes.batch.recon.sensor;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.transaction.PlatformTransactionManager;


@Configuration
public class SensorDataConfig {

    @Value("classpath:input/HTE2NP.txt")
    private Resource dataFile;

    @Value("file:HTE2NP.xml")
    private WritableResource aggregatedDailyOutputXmlResource;

    @Value("file:HTE2NP-anomalies.csv")
    private WritableResource anomalyDataResource;


    @Bean
    public Job temperatureSensorJob(JobRepository jobRepository,
                                    @Qualifier("aggregateSensorStep") Step aggregateSensorStep,
                                    @Qualifier("reportAnomaliesStep") Step reportAnomaliesStep) {
        return new JobBuilder("temperatureSensorJob", jobRepository)
                .start(aggregateSensorStep)
                .next(reportAnomaliesStep)
                .build();
    }

    @Bean
    public FlatFileItemReader<DailySensorData> sensorDataFilereader() {
        return new FlatFileItemReaderBuilder<DailySensorData>()
                .name("sensorDataReader")
                .resource(dataFile)
                .lineMapper(new SensorDataTextMapper())
                .build();
    }
    
    
    @Bean("aggregateSensorStep")
    public Step aggregateSensorStep(JobRepository jobRepository, PlatformTransactionManager txManager) {
        return new StepBuilder("aggregate-sensor", jobRepository)
                .<DailySensorData,DailyAggregatedSensorData>chunk(1, txManager)
                .reader(sensorDataFilereader())
                .processor(new RawToAggregateSensorDataProcessor())
                .writer(new StaxEventItemWriterBuilder<DailyAggregatedSensorData>()
                        .name("dailyAggregatedSensorDataWriter")
                        .marshaller(DailyAggregatedSensorData.getMarshaller())
                        .resource(aggregatedDailyOutputXmlResource)
                        .rootTagName("data")
                        .overwriteOutput(true)
                        .build())
                .build();
    }

    @Bean("reportAnomaliesStep")
    public Step reportAnomaliesStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("report-anomalies", jobRepository)
                // Reading in chunks of size 1, item-by-item
                .<DailyAggregatedSensorData, DataAnomaly>chunk(1, transactionManager)
                // Reading from XML file re-using the same marshaller as for writing
                .reader(new StaxEventItemReaderBuilder<DailyAggregatedSensorData>()
                        .name("dailyAggregatedSensorDataReader")
                        .unmarshaller(DailyAggregatedSensorData.getMarshaller())
                        .resource(aggregatedDailyOutputXmlResource)
                        .addFragmentRootElements(DailyAggregatedSensorData.ITEM_ROOT_ELEMENT_NAME)
                        .build()
                )
                .processor(new SensorDataAnomalyProcessor())
                // Writing comma-delimited CSV format
                .writer(new FlatFileItemWriterBuilder<DataAnomaly>()
                        .name("dataAnomalyWriter")
                        .resource(anomalyDataResource)
                        .delimited()
                        .delimiter(",")
                        .names(new String[] {"date", "type", "value"})
                        .build()
                )
                .build();
    }



}
