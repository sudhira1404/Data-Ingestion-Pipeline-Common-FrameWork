package com.target.kelsaapi.common.service.file;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

/**
 * Interface for Writing messages/events/records
 *
 * @since 1.0
 */
public interface HDFSFileWriterService {
    Boolean writeToHDFS(String contents,String filePath);

    Boolean writeToHDFS(String contents,String filePath, Integer writeAttempts, String tempFile, Boolean cleanupTempFile);

    Boolean writeToHDFS(List<String> contents, String filePath, Integer writeAttempts, String tempFile, Boolean cleanupTempFile);

    Boolean writeToHDFS(String filePath, String outputGZTarFile, List<Path> inputFilePaths, Integer writeAttempts,Boolean cleanupTempFile);

    Boolean writeToHDFS(String filePath, Integer writeAttempts, String tempFile, Boolean cleanupTempFile);

    Boolean writeToHDFS(InputStream contents, String filePath, Integer writeAttempts, String tempFile, Boolean cleanupTempFile);

    void writeToHDFS(String filePath ,String tempFile, Integer writeAttempts,Boolean cleanupTempFile) throws IOException;
}
