package io.javabytes.batch.recon;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

//exclude = {DataSourceAutoConfiguration.class}
@SpringBootApplication()
public class ReconApplication {

	public static void main(String[] args) {
		SpringApplication.run(ReconApplication.class, args);
	}

}
