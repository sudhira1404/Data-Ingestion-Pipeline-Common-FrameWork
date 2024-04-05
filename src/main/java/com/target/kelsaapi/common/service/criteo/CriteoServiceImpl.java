package com.target.kelsaapi.common.service.criteo;

import com.jcraft.jsch.*;
import com.target.kelsaapi.common.vo.criteo.CriteoRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;

@Slf4j
@Service
public class CriteoServiceImpl implements CriteoService {

    public String getCriteoData(CriteoRequest request, String hostName, String userName, String password, String privateKeyFile, String pipelineRunId, String reportType, String remoteDirectory, String remoteFileName) throws JSchException, SftpException {
        log.info("connecting to the host: " + hostName);
        try {
            ChannelSftp channelSftp = setupJsch(userName, password, privateKeyFile, hostName, 22);
            channelSftp.connect();

            log.info("Started pulling the file from the host: " + hostName);
            // download file from remote server to local
            channelSftp.get(remoteFileName, remoteFileName);
            log.info("Finished pulling the file from the host: " + hostName);
            return remoteFileName;
        } catch (JSchException e) {
            log.error(e.getMessage(),e.getCause());
            log.error(Arrays.toString(e.getStackTrace()));
            throw new JSchException(e.getMessage(),e.getCause());
        }
        catch (SftpException e) {
            log.error(e.getMessage(),e.getCause());
            log.error(Arrays.toString(e.getStackTrace()));
            throw new SftpException(e.id,e.getMessage());
        }
    }

    private ChannelSftp setupJsch(String username, String password, String pvKeyPath, String hostname, int port) throws JSchException {
        JSch jsch = new JSch();
        if (!pvKeyPath.equals("") && pvKeyPath != null)
            jsch.addIdentity(pvKeyPath);

        Session jschSession = jsch.getSession(username, hostname, port);
        if (!password.equals("") && password != null)
            jschSession.setPassword(password);

        jschSession.setConfig("StrictHostKeyChecking", "no");
        jschSession.connect();
        return (ChannelSftp) jschSession.openChannel("sftp");
    }

}
