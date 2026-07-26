package io.javabytes.batch.s3sftp.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;

@Configuration
@EnableConfigurationProperties(S3SftpProperties.class)
public class S3ClientConfig {

    @Bean
    public S3Client s3Client(S3SftpProperties properties) {
        S3SftpProperties.S3 s3Props = properties.getS3();

        return S3Client.builder()
                .region(Region.of(s3Props.getRegion()))
                .endpointOverride(URI.create(s3Props.getEndpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(s3Props.getAccessKey(), s3Props.getSecretKey())))
                // NetApp StorageGRID (and most on-prem S3-compatible targets) require path-style
                // addressing since they don't support virtual-hosted-style DNS buckets.
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(s3Props.isPathStyleAccess())
                        .build())
                .build();
    }
}
