package io.javabytes.batch.s3sftp.tasklet;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import io.javabytes.batch.s3sftp.config.S3SftpProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.retry.support.RetryTemplate;

import java.nio.file.Path;
import java.util.Properties;
import java.util.Vector;

/**
 * Uploads the enriched output file to the SFTP target.
 *
 * Uploads to a ".part" name first, then renames server-side to the final name once the transfer
 * completes. That way a partner process polling the remote directory never picks up a
 * half-written file, and if the job fails mid-transfer, a restart simply re-uploads the .part
 * file cleanly (the incomplete file is never visible under its real name).
 */
public class SftpUploadTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(SftpUploadTasklet.class);

    private final S3SftpProperties.Sftp sftpProps;
    private final RetryTemplate retryTemplate;
    private final Path localFile;
    private final String remoteFileName;

    public SftpUploadTasklet(S3SftpProperties.Sftp sftpProps, RetryTemplate retryTemplate,
                              Path localFile, String remoteFileName) {
        this.sftpProps = sftpProps;
        this.retryTemplate = retryTemplate;
        this.localFile = localFile;
        this.remoteFileName = remoteFileName;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        retryTemplate.execute(context -> {
            log.info("Uploading {} to sftp://{}:{}{}/{} (attempt {})",
                    localFile, sftpProps.getHost(), sftpProps.getPort(), sftpProps.getRemoteDir(),
                    remoteFileName, context.getRetryCount() + 1);
            upload();
            return null;
        });
        return RepeatStatus.FINISHED;
    }

    private void upload() throws Exception {
        Session session = null;
        ChannelSftp channel = null;
        try {
            JSch jsch = new JSch();
            if (sftpProps.getPrivateKeyPath() != null) {
                jsch.addIdentity(sftpProps.getPrivateKeyPath());
            }
            if (sftpProps.getKnownHostsPath() != null) {
                jsch.setKnownHosts(sftpProps.getKnownHostsPath());
            }

            session = jsch.getSession(sftpProps.getUser(), sftpProps.getHost(), sftpProps.getPort());
            if (sftpProps.getPassword() != null) {
                session.setPassword(sftpProps.getPassword());
            }
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", sftpProps.isStrictHostKeyChecking() ? "yes" : "no");
            session.setConfig(config);
            session.setTimeout(sftpProps.getConnectTimeoutMillis());
            session.connect();

            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();
            channel.cd(sftpProps.getRemoteDir());

            String partName = remoteFileName + ".part";
            channel.put(localFile.toString(), partName);

            // Server-side atomic-ish rename: partner never sees a half-uploaded file.
            removeIfExists(channel, remoteFileName);
            channel.rename(partName, remoteFileName);

            log.info("Upload complete: {}", remoteFileName);
        } finally {
            if (channel != null) channel.disconnect();
            if (session != null) session.disconnect();
        }
    }

    @SuppressWarnings("unchecked")
    private void removeIfExists(ChannelSftp channel, String fileName) {
        try {
            Vector<ChannelSftp.LsEntry> entries = channel.ls(fileName);
            if (entries != null && !entries.isEmpty()) {
                channel.rm(fileName);
            }
        } catch (Exception e) {
            // ls throws if the file doesn't exist - that's the expected, common case.
        }
    }
}
