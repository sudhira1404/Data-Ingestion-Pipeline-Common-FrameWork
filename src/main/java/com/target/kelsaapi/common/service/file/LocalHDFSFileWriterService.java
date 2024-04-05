package com.target.kelsaapi.common.service.file;

import java.io.IOException;
import java.net.URL;

public interface LocalHDFSFileWriterService {

    void writeLocalFileToHdfs(URL downloadURL, String localFilePathNoExtension,
                              String localFileExtension,String tempFileDirectory,String targetFile, Boolean compress,
                               Integer maxLinesBeforeFlush, Long maxSizeBeforeNewFile
    ) throws IOException;

}
