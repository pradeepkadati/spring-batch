package io.javabytes.batch.recon.grade;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class GradeJobRunner implements CommandLineRunner {

    private final JobLauncher jobLauncher;
    private final Job gradeJob; // This matches the bean name in your config

    public GradeJobRunner(JobLauncher jobLauncher, Job gradeJob) {
        this.jobLauncher = jobLauncher;
        this.gradeJob = gradeJob;
    }

    @Override
    public void run(String... args) throws Exception {
        // We can extract a custom parameter from the command line args
        // Usage: ./gradlew bootRun --args="version=1.0"
        String version = (args.length > 0) ? args[0] : "default";

        JobParameters params = new JobParametersBuilder()
                .addString("version", version)
                .addLong("time", System.currentTimeMillis()) // Unique param to allow re-runs
                .toJobParameters();

        System.out.println("--- Starting Job via Command Line ---");
        jobLauncher.run(gradeJob, params);
        System.out.println("--- Job Finished ---");
    }

}
