package io.javabytes.batch.recon;

import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

//./gradlew bootRun --args="job=gradeJob"

@Component
public class MultiJobRunner implements CommandLineRunner {

    private final JobLauncher jobLauncher;
    private final Map<String, Job> jobs; // Spring injects all Job beans here

    public MultiJobRunner(JobLauncher jobLauncher, Map<String, Job> jobs) {
        this.jobLauncher = jobLauncher;
        this.jobs = jobs;
    }

    @Override
    public void run(String... args) throws Exception {
        // Logic: Look for an argument like "job=gradeJob"
        String jobToRun = null;
        for (String arg : args) {
            if (arg.startsWith("job=")) {
                jobToRun = arg.split("=")[1];
            }
        }

        if (jobToRun == null) {
            System.out.println("Error: Please provide a job name. Example: --args='job=gradeJob'");
            System.out.println("Available jobs: " + jobs.keySet());
            return;
        }

        Job job = jobs.get(jobToRun);

        if (job != null) {
            System.out.println(">>> Launching Job: " + jobToRun);
            jobLauncher.run(job, new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .toJobParameters());
        } else {
            System.out.println("Error: Job '" + jobToRun + "' not found.");
            System.out.println("Available jobs: " + jobs.keySet());
        }
    }

}
