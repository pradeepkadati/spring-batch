package io.javabytes.batch.failedjobs.restart;
import com.example.batch.processor.SampleItemProcessor;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
public class BatchConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;

    /**
     * Primary synchronous JobLauncher — used by the poller.
     * Returns once the job completes (or fails), so the poller
     * can log the outcome immediately.
     */
    @Bean
    @Primary
    public JobLauncher jobLauncher() throws Exception {
        TaskExecutorJobLauncher launcher = new TaskExecutorJobLauncher();
        launcher.setJobRepository(jobRepository);
        launcher.setTaskExecutor(new SimpleAsyncTaskExecutor("batch-restart-"));
        launcher.afterPropertiesSet();
        return launcher;
    }

    @Bean
    public Job sampleJob(Step processStep) {
        return new JobBuilder("sampleJob", jobRepository)
            .start(processStep)
            .build();
    }

    @Bean
    public Step processStep(FlatFileItemReader<SampleItem> reader,
                            SampleItemProcessor processor,
                            JdbcBatchItemWriter<SampleItem> writer) {
        return new StepBuilder("processStep", jobRepository)
            .<SampleItem, SampleItem>chunk(100, transactionManager)
            .reader(reader)
            .processor(processor)
            .writer(writer)
            // Allow up to 3 skippable exceptions before the step itself fails
            .faultTolerant()
            .skipLimit(3)
            .skip(IllegalArgumentException.class)
            .build();
    }

    @Bean
    public FlatFileItemReader<SampleItem> itemReader() {
        return new FlatFileItemReaderBuilder<SampleItem>()
            .name("sampleItemReader")
            .resource(new ClassPathResource("data/input.csv"))
            .delimited()
            .names("id", "name", "value")
            .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
                setTargetType(SampleItem.class);
            }})
            .build();
    }

    @Bean
    public JdbcBatchItemWriter<SampleItem> itemWriter() {
        return new JdbcBatchItemWriterBuilder<SampleItem>()
            .dataSource(dataSource)
            .sql("INSERT INTO processed_items (id, name, value) " +
                 "VALUES (:id, :name, :value) " +
                 "ON CONFLICT (id) DO UPDATE SET name=:name, value=:value")
            .beanMapped()
            .build();
    }

}
