package io.javabytes.batch.recon.grade;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class GradeBatchConfig {


    @Bean
    public FlatFileItemReader<Student> reader() {
        return new FlatFileItemReaderBuilder<Student>()
                .name("studentReader")
                .resource(new ClassPathResource("students.csv"))
                .delimited()
                .delimiter(",")
                .names("name", "score","grade")
                .targetType(Student.class)
                .build();
    }

    @Bean
    public FlatFileItemReader<Student> readerLineTokenizer() {
        return new FlatFileItemReaderBuilder<Student>()
                .name("studentReaderWithLineTokenizer")
                .lineTokenizer(new DelimitedLineTokenizer(","))
                .resource(new ClassPathResource("students.csv"))
                .fieldSetMapper(fieldSet -> {
                    Student student = new Student(fieldSet.readString(0), fieldSet.readInt(1));
                    return student;
                }).build();
    }

    @Bean
    public GradeProcessor processor() {
        return new GradeProcessor();
    }

    @Bean
    public Step gradeStep(JobRepository jobRepository, PlatformTransactionManager txManager) {
        return new StepBuilder("gradeStep", jobRepository)
                .<Student, Student>chunk(10, txManager)
                //.reader(reader())
                .reader(readerLineTokenizer())
                .processor(processor())
                .writer(chunk -> chunk.forEach(s -> 
                    System.out.println("FINAL RESULT: " + s.name() + " got an " + s.grade())))
                .build();
    }

    @Bean
    public Job gradeJob(JobRepository jobRepository, Step gradeStep) {
        return new JobBuilder("gradeJob", jobRepository)
                .start(gradeStep)
                .build();
    }

}
