package com.target.kelsaapi.common.service.file;

import com.target.kelsaapi.common.exceptions.UnImplementedException;
import org.springframework.lang.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;

public interface LocalFileWriterService {
    Boolean writeLocalFile(String contents, String localFilePath);

    Boolean writeLocalFile(String contents, String localFilePath, Boolean compress, Boolean append);

    Boolean writeLocalFile(List<String> contents, String localFilePath);

    Boolean writeLocalFile(List<String> contents, String localFilePath, Boolean compress);

    Boolean writeLocalFile(List<String> contents, String localFilePath, Boolean compress, Boolean append);

    Boolean writeLocalFile(String outputGZTarFile, List<Path> inputFilePaths);

    Boolean writeLocalFile(InputStream inputStream, String localFilePath, Boolean compress);

    Boolean writeLocalFile(InputStream inputStream, String localFilePath, Boolean compress, Boolean append);

    List<Path> writeLocalFile(URL downloadURL, String localFilePathNoExtension,
                           String localFileExtension, Boolean compress,
                           @Nullable Integer maxLinesBeforeFlush, @Nullable Integer maxLinesBeforeNewFile,
                           Boolean tarFiles);

    Boolean writeLocalFile(InputStream inputStream, String localFilePath);

    void deleteLocalFile(Path inputFile);

    Boolean retryableLocalWrite(Object contents, Object tempFile) throws UnImplementedException;

    List<Path> findFilesFromNamePrefix(String filePrefix) throws IOException;

    void deleteLocalFiles(List<Path> inputFiles);
}
