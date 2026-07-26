package io.javabytes.batch.s3sftp.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.file.Path;

@ConfigurationProperties(prefix = "batch.s3sftp")
public class S3SftpProperties {

    private final S3 s3 = new S3();
    private final Sftp sftp = new Sftp();
    private final Local local = new Local();
    private final Retry retry = new Retry();

    public S3 getS3() { return s3; }
    public Sftp getSftp() { return sftp; }
    public Local getLocal() { return local; }
    public Retry getRetry() { return retry; }

    public static class S3 {
        /** NetApp StorageGRID S3-compatible endpoint, e.g. https://storagegrid.internal.company.com */
        private String endpoint;
        private String region = "us-east-1";
        private String accessKey;
        private String secretKey;
        private boolean pathStyleAccess = true;
        private String bucket;
        /** Prefix polled for new inbound files, e.g. "inbound/" */
        private String inboundPrefix = "inbound/";
        private String processedPrefix = "processed/";
        private String failedPrefix = "failed/";

        // getters/setters
        public String getEndpoint() { return endpoint; }
        public void setEndpoint(String endpoint) { this.endpoint = endpoint; }
        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }
        public String getAccessKey() { return accessKey; }
        public void setAccessKey(String accessKey) { this.accessKey = accessKey; }
        public String getSecretKey() { return secretKey; }
        public void setSecretKey(String secretKey) { this.secretKey = secretKey; }
        public boolean isPathStyleAccess() { return pathStyleAccess; }
        public void setPathStyleAccess(boolean pathStyleAccess) { this.pathStyleAccess = pathStyleAccess; }
        public String getBucket() { return bucket; }
        public void setBucket(String bucket) { this.bucket = bucket; }
        public String getInboundPrefix() { return inboundPrefix; }
        public void setInboundPrefix(String inboundPrefix) { this.inboundPrefix = inboundPrefix; }
        public String getProcessedPrefix() { return processedPrefix; }
        public void setProcessedPrefix(String processedPrefix) { this.processedPrefix = processedPrefix; }
        public String getFailedPrefix() { return failedPrefix; }
        public void setFailedPrefix(String failedPrefix) { this.failedPrefix = failedPrefix; }
    }

    public static class Sftp {
        private String host;
        private int port = 22;
        private String user;
        private String password;
        private String privateKeyPath;
        private String remoteDir = "/inbound/enriched";
        private int connectTimeoutMillis = 15_000;
        private boolean strictHostKeyChecking = true;
        private String knownHostsPath;

        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public String getUser() { return user; }
        public void setUser(String user) { this.user = user; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public String getPrivateKeyPath() { return privateKeyPath; }
        public void setPrivateKeyPath(String privateKeyPath) { this.privateKeyPath = privateKeyPath; }
        public String getRemoteDir() { return remoteDir; }
        public void setRemoteDir(String remoteDir) { this.remoteDir = remoteDir; }
        public int getConnectTimeoutMillis() { return connectTimeoutMillis; }
        public void setConnectTimeoutMillis(int connectTimeoutMillis) { this.connectTimeoutMillis = connectTimeoutMillis; }
        public boolean isStrictHostKeyChecking() { return strictHostKeyChecking; }
        public void setStrictHostKeyChecking(boolean strictHostKeyChecking) { this.strictHostKeyChecking = strictHostKeyChecking; }
        public String getKnownHostsPath() { return knownHostsPath; }
        public void setKnownHostsPath(String knownHostsPath) { this.knownHostsPath = knownHostsPath; }
    }

    public static class Local {
        private Path stagingDir = Path.of(System.getProperty("java.io.tmpdir"), "s3sftp-staging");
        private boolean keepFilesOnFailure = true;

        public Path getStagingDir() { return stagingDir; }
        public void setStagingDir(Path stagingDir) { this.stagingDir = stagingDir; }
        public boolean isKeepFilesOnFailure() { return keepFilesOnFailure; }
        public void setKeepFilesOnFailure(boolean keepFilesOnFailure) { this.keepFilesOnFailure = keepFilesOnFailure; }
    }

    public static class Retry {
        private int maxAttempts = 4;
        private long initialBackoffMillis = 500;
        private double backoffMultiplier = 2.0;
        private long maxBackoffMillis = 8_000;
        private int chunkSize = 100;
        private int skipLimit = 20;
        private int processRetryLimit = 3;

        public int getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
        public long getInitialBackoffMillis() { return initialBackoffMillis; }
        public void setInitialBackoffMillis(long initialBackoffMillis) { this.initialBackoffMillis = initialBackoffMillis; }
        public double getBackoffMultiplier() { return backoffMultiplier; }
        public void setBackoffMultiplier(double backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }
        public long getMaxBackoffMillis() { return maxBackoffMillis; }
        public void setMaxBackoffMillis(long maxBackoffMillis) { this.maxBackoffMillis = maxBackoffMillis; }
        public int getChunkSize() { return chunkSize; }
        public void setChunkSize(int chunkSize) { this.chunkSize = chunkSize; }
        public int getSkipLimit() { return skipLimit; }
        public void setSkipLimit(int skipLimit) { this.skipLimit = skipLimit; }
        public int getProcessRetryLimit() { return processRetryLimit; }
        public void setProcessRetryLimit(int processRetryLimit) { this.processRetryLimit = processRetryLimit; }
    }
}
