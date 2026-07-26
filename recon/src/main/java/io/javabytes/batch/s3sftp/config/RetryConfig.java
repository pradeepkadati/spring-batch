package io.javabytes.batch.s3sftp.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import software.amazon.awssdk.core.exception.SdkException;

import java.io.IOException;
import java.util.Map;

/**
 * A single RetryTemplate reused by the S3 download and SFTP upload tasklets, so both
 * infrastructure hops (NetApp S3 and the SFTP target) get the same backoff behaviour
 * for transient network/IO failures. Business/validation errors never go through this -
 * those are handled as chunk-level skips, not retries.
 */
@Configuration
@EnableConfigurationProperties(S3SftpProperties.class)
public class RetryConfig {

    @Bean
    public RetryTemplate infrastructureRetryTemplate(S3SftpProperties properties) {
        S3SftpProperties.Retry retryProps = properties.getRetry();

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(retryProps.getInitialBackoffMillis());
        backOffPolicy.setMultiplier(retryProps.getBackoffMultiplier());
        backOffPolicy.setMaxInterval(retryProps.getMaxBackoffMillis());

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(
                retryProps.getMaxAttempts(),
                Map.of(
                        SdkException.class, true,
                        IOException.class, true,
                        com.jcraft.jsch.JSchException.class, true,
                        com.jcraft.jsch.SftpException.class, true
                ),
                true);

        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setBackOffPolicy(backOffPolicy);
        retryTemplate.setRetryPolicy(retryPolicy);
        return retryTemplate;
    }
}
