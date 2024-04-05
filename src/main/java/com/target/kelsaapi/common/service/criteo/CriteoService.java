package com.target.kelsaapi.common.service.criteo;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import com.target.kelsaapi.common.vo.criteo.CriteoRequest;

import java.io.IOException;

public interface CriteoService {
    String getCriteoData(CriteoRequest request, String hostName, String userName, String password, String privateKeyFile, String pipelineRunId, String reportType, String remoteDirectory, String remoteFileName) throws IOException, JSchException, SftpException;
}
