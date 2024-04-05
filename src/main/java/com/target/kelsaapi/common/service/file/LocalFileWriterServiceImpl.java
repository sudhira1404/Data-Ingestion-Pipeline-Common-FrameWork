package com.target.kelsaapi.common.service.file;

import com.google.api.client.util.Lists;
import com.target.kelsaapi.common.exceptions.UnImplementedException;
import com.target.kelsaapi.common.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Implementation of LocalFileWriterService that contains all methods pertaining to local file writing.
 */
@Slf4j
@Service
public class LocalFileWriterServiceImpl implements LocalFileWriterService{

    /**
     * Writes a String to a local file. Uses {@link #writeLocalFile(InputStream, String)} for writing and
     * checking the final results match the input.
     *
     * @param contents The String to write.
     * @param localFilePath The file to write to.
     * @return True if successful; False if unsuccessful.
     */
    @Override
    public Boolean writeLocalFile(String contents, String localFilePath) {
        InputStream myContents = new ByteArrayInputStream(contents.getBytes(StandardCharsets.UTF_8));
        return writeLocalFile(myContents, localFilePath);
    }
    @Override
    public Boolean writeLocalFile(String contents, String localFilePath, Boolean compress, Boolean append) {
        InputStream myContents = new ByteArrayInputStream(contents.getBytes(StandardCharsets.UTF_8));
        return writeLocalFile(myContents, localFilePath, compress, append);
    }
    /**
     * Equivalent to {@link #writeLocalFile(List, String, Boolean)} with the compress option set to True.
     *
     * @param contents The List of Strings to write.
     * @param localFilePath The file to write to.
     * @return True if successful; False if unsuccessful.
     */
    @Override
    public Boolean writeLocalFile(List<String> contents, String localFilePath) {
        return writeLocalFile(contents, localFilePath, true);
    }

    /**
     * Writes a List of Strings to a local file. Inserts a newline character at the end of each row.
     *
     * @param contents The List of Strings to write.
     * @param localFilePath The file to write to.
     * @param compress Whether to gzip the file or not.
     * @return True if successful; False if unsuccessful.
     */
    @Override
    public Boolean writeLocalFile(List<String> contents, String localFilePath, Boolean compress) {
        return writeLocalFile(contents, localFilePath, compress, false);
    }

    /**
     * Writes a List of Strings to a local file. Inserts a newline character at the end of each row.
     *
     * @param contents The List of Strings to write.
     * @param localFilePath The file to write to.
     * @param compress Whether to gzip the file or not.
     * @return True if successful; False if unsuccessful.
     */
    @Override
    public Boolean writeLocalFile(List<String> contents, String localFilePath, Boolean compress, Boolean append) {
        if (Boolean.TRUE.equals(compress)) {
            return writeCompressedListFile(contents, localFilePath, append);
        } else {
            return writeListFile(contents, localFilePath, append);
        }

    }

    /**
     * Uses {@link GzipCompressorOutputStream} to write to a local file. Adds a newline character at the end of each
     * String in the List.
     *
     * @param contents The List of Strings to write.
     * @param localFilePath The file to write to.
     * @return True if successful; False if unsuccessful.
     */
    private Boolean writeCompressedListFile(List<String> contents, String localFilePath, Boolean append) {
        try (GzipCompressorOutputStream bout = new GzipCompressorOutputStream(new FileOutputStream(localFilePath, append));) {
            writeListFile(contents, bout);
        } catch (IOException e) {
            log.error("Unable to write to local temp file : " + localFilePath);
            log.error(e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * Uses {@link BufferedOutputStream} to write to a local file. Adds a newline character at the end of each String
     * in the List.
     *
     * @param contents The List of Strings to write.
     * @param localFilePath The file to write to.
     * @return True if successful; False if unsuccessful.
     */
    private Boolean writeListFile(List<String> contents, String localFilePath, Boolean append) {
        try (BufferedOutputStream bout = new BufferedOutputStream(new FileOutputStream(localFilePath, append));) {
            writeListFile(contents, bout);
        } catch (IOException e) {
            log.error("Unable to write to local temp file : " + localFilePath);
            log.error(e.getMessage());
            return false;
        }
        return true;
    }

    private void writeListFile(List<String> contents, OutputStream os) throws IOException {
        for (String s : contents) {
            s += System.getProperty("line.separator");
            os.write(s.getBytes());
        }
    }

    /**
     * Adds all files at the given path to a new tar file and gzip the tar file.
     *
     * @param outputGZTarFile The final tar and gzipped file to produce.
     * @param inputFilePaths A path to a directory to one or more files.
     *                       These files will be moved into the tar file, and then deleted.
     * @return True if successful; False if unsuccessful.
     */
    @Override
    public Boolean writeLocalFile(String outputGZTarFile, List<Path> inputFilePaths) {
        Path outputGZTarFilePath = Paths.get(outputGZTarFile);
        log.info("File tar process started");
        try(OutputStream outputStream = Files.newOutputStream(outputGZTarFilePath);
            GzipCompressorOutputStream gzipOut = new GzipCompressorOutputStream(outputStream);
            TarArchiveOutputStream tarOut = new TarArchiveOutputStream(gzipOut)) {

                for (Path inputFile : inputFilePaths) {
                    File inFile = inputFile.toFile();
                    log.debug("Input file length : {}", inFile.length());
                    log.debug("Input file path : {}", inFile.getPath());
                    log.debug("Input file name : {}", inFile.getName());
                    log.debug("Input file parent : {}", inFile.getParent());
                    if (inFile.length() > 0) {
                        tarOut.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
                        tarOut.putArchiveEntry(tarOut.createArchiveEntry(inFile, inFile.getName()));
                        Files.copy(inputFile, tarOut);
                        tarOut.closeArchiveEntry();
                        deleteLocalFile(inputFile);
                    }
                }
                tarOut.finish();
                log.info("File tar process ended. Generated Tar file: " +outputGZTarFilePath );
                return true;
            } catch (IOException e) {
            log.error("Exception thrown in writing Tar Gzip file {}", e.getMessage());
            return false;
        }
    }
    @Override
    public Boolean writeLocalFile(InputStream inputStream, String localFilePath, Boolean compress) {
        return writeLocalFile(inputStream, localFilePath, compress, false);
    }

    /**
     * Writes a given InputStream to a local gzip file. This leverages {@link IOUtils#copy(InputStream, OutputStream)} method
     * for buffering.
     *
     * @param inputStream The stream to write to file.
     * @param localFilePath The file to write to.
     * @param compress True to use gzip compression on the file; false otherwise.
     * @return True if successful; False if unsuccessful.
     */
    @Override
    public Boolean writeLocalFile(InputStream inputStream, String localFilePath, Boolean compress, Boolean append) {
        if (compress) {
            try (FileOutputStream fileOutputStream = new FileOutputStream(localFilePath, append);
                 GzipCompressorOutputStream gzipOutputStream =  new GzipCompressorOutputStream(fileOutputStream);
            ) {
                log.info("Begin buffered writing to {}", localFilePath);
                IOUtils.copyLarge(inputStream, gzipOutputStream);
            } catch (IOException e) {
                log.error("Failed while buffered writing to {}", localFilePath);
                return false;
            }
        } else {
            try (FileOutputStream fileOutputStream = new FileOutputStream(localFilePath, append)) {
                log.info("Begin buffered writing to {}", localFilePath);
                IOUtils.copyLarge(inputStream, fileOutputStream);
            } catch (IOException e) {
                log.error("Failed while buffered writing to {}", localFilePath);
                return false;
            }
        }
        Boolean fileExists = Files.exists(Paths.get(localFilePath));
        if (Boolean.TRUE.equals(fileExists)) {
            log.info("Successfully wrote file to {}", localFilePath);
            return true;
        } else {
            log.error("File doesn't exist after attempt to write: {}", localFilePath);
            return false;
        }
    }

    /**
     * Streaming writer from URL to local tar gzipped file reading each line of the input source.
     * The stream is read into smaller lists, and when the  maximum lines configured for flush have been reached,
     * contents are flushed to a raw file. A new raw file is created after the maximum lines configured for new file
     * is reached. The first record from the stream is considered the header and will be repeated at the beginning of
     * each new raw file. Once all lines from the stream have been read, all raw files will be moved into a final
     * tar file and gzipped locally.
     *
     * @param downloadURL The {@link URL} to open the stream against and read lines.
     * @param localFilePathNoExtension The root of the file path used for all raw files and the final tar file.
     * @param localFileExtension The extension of the raw files, typically ".csv"
     * @param compress True to compress the raw files before moving them to the tar file; false to only gzip the tar file.
     * @param maxLinesBeforeFlush Sets the maximum size of the List object used to buffer the stream before flushing to a raw file.
     * @param maxLinesBeforeNewFile Sets the maximum size each raw file can grow before a new file will be started.
     * @return True if all contents are successfully written to the final .tar.gz file; false otherwise.
     */
    @Override
    public List<Path> writeLocalFile(URL downloadURL, String localFilePathNoExtension,
                                  String localFileExtension, Boolean compress,
                                  @Nullable Integer maxLinesBeforeFlush, @Nullable Integer maxLinesBeforeNewFile,
                                  Boolean tarFiles) {
        if (maxLinesBeforeFlush == null) maxLinesBeforeFlush = 10000;
        if (maxLinesBeforeNewFile == null) maxLinesBeforeNewFile = 5000000;
        int flush = 1;
        int newFile = 1;
        int newFileLineCounter = 1;
        int totalLineCounter = 1;
        log.debug("URL to download from : {}", downloadURL.toExternalForm());

        try (InputStream inputStream = downloadURL.openStream();
             InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
             BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        ) {
            List<String> lines = null;
            List<Path> files = Lists.newArrayList();
            String line;
            String fileName = localFilePathNoExtension + "_" + newFile + localFileExtension;
            String header = bufferedReader.readLine();
            log.debug("Header : {}", header);
            while((line = bufferedReader.readLine()) != null ) {
                if (flush == 1) {
                    lines = Lists.newArrayList();
                    if (newFileLineCounter == 1) {
                        log.debug("Adding Header to the beginning of a new file");
                        lines.add(header);
                    }
                }
                lines.add(line);
                flush++;
                newFileLineCounter++;
                totalLineCounter++;
                if (maxLinesBeforeNewFile.equals(newFileLineCounter)) {
                    Boolean writeCheck;
                    if (compress) {
                        writeCheck = writeCompressedListFile(lines, fileName, true);
                    } else {
                        writeCheck = writeListFile(lines, fileName, true);
                    }
                    if (writeCheck) {
                        log.info("Successfully wrote {} lines to file {}", newFileLineCounter, fileName);
                        files.add(Paths.get(fileName));
                        flush = 1;
                        newFileLineCounter = 1;
                        newFile++;
                        fileName = localFilePathNoExtension + "_" + newFile + localFileExtension;
                        log.info("Maximum file size reached, will begin buffered writing to new file {} on the next line read",
                                fileName);
                    } else {
                        throw new IOException("Failed on final write to file " + fileName);
                    }
                }

                if (maxLinesBeforeFlush.equals(flush)) {
                    Boolean writeCheck;
                    if (compress) {
                        writeCheck = writeCompressedListFile(lines, fileName, true);
                    } else {
                        writeCheck = writeListFile(lines, fileName, true);
                    }
                    if (writeCheck) {
                        log.debug("Successfully flushed {} lines to file {}", flush, fileName);
                        flush = 1;
                    } else {
                        throw new IOException("Failed to append line buffer to file " + fileName);
                    }
                }
            }

            if (lines != null && !lines.isEmpty()) {
                Boolean writeCheck;
                if (compress) {
                    writeCheck = writeCompressedListFile(lines, fileName, true);
                } else {
                    writeCheck = writeListFile(lines, fileName, true);
                }
                if (writeCheck) {
                    log.info("Successfully flushed final {} lines to file {}", flush, fileName);
                    files.add(Paths.get(fileName));
                } else {
                    throw new IOException("Failed to append final line buffer to file " + fileName);
                }
            } else {
                throw new IOException("There were no lines of data in the downloaded file from the URL: " + downloadURL);
            }
            if (tarFiles) {
                log.debug("Moving all {} files into tar gzipped archive", newFile);
                String tarGzipArchive = localFilePathNoExtension + localFileExtension + ".tar.gz";
                if (writeLocalFile(tarGzipArchive, files)) {
                    log.info("Successfully compressed and moved all {} files into tar gzip archive {}", newFile, tarGzipArchive);
                } else {
                    throw new IOException("Failed to move files into tar gzip archive " + tarGzipArchive);
                }

                log.info("Successfully wrote all {} lines to {} local files", totalLineCounter, newFile);
                List<Path> finalFile = Lists.newArrayList();
                finalFile.add(Paths.get(tarGzipArchive));
                return finalFile;
            } else {
                return files;
            }

        } catch (IOException e) {
            log.error(e.getMessage(), e.getCause());
            return Lists.newArrayList();
        }
    }

    /**
     * Writes a given InputStream to a local gzip file. This leverages {@link IOUtils#copy(InputStream, OutputStream)} method
     * for buffering.
     *
     * @param inputStream The stream to write to file.
     * @param localFilePath The file to write to.
     * @return True if successful; False if unsuccessful.
     */
    @Override
    public Boolean writeLocalFile(InputStream inputStream, String localFilePath) {
        return writeLocalFile(inputStream, localFilePath, true);
    }

    /**
     * Deletes a local file. Purposely does not return anything, only logs whether the file was removed or not. Motivation
     * behind this decision is due to the fact that in general the app should not stop functioning if a local file
     * cannot be deleted, since this is intended to be called after the file has already been transferred from
     * local to remote filesystem. Since local is ephemeral, a restart of the container will remove any orphaned files.
     *
     * @param inputFile The local file to delete.
     */
    @Override
    public void deleteLocalFile(Path inputFile) {
        try {
            Files.deleteIfExists(inputFile);
            log.info("Successfully cleaned up temp file on local filesystem: {}", inputFile.toFile().getPath());
        } catch (IOException e) {
            log.error("Unable to delete temp file, manual intervention may be needed on local filesystem: {}", inputFile.toFile().getPath());
        }
    }

    /**
     * Wrapper class around all writeLocalFile methods that retries up to 3 times before returning false.
     *
     * @param contents The contents to write to file.
     * @param tempFile The file to write to.
     * @return True if successful (after any possible retries); false otherwise.
     * @throws UnImplementedException Thrown when an unmatched type is detected for either or both of the arguments.
     */
    @Override
    public Boolean retryableLocalWrite(Object contents, Object tempFile) throws UnImplementedException {
        Boolean localFileWriteSuccess = false;
        int attempts = 1;
        int maxAttempts = 3;
        while (Boolean.FALSE.equals(localFileWriteSuccess) && attempts <= maxAttempts) {
            if (contents instanceof String && tempFile instanceof String) {
                localFileWriteSuccess = writeLocalFile((String) contents, (String) tempFile);
            } else if (contents instanceof List) {
                localFileWriteSuccess = writeLocalFile((List) contents, (String) tempFile);
            } else if (contents instanceof String && tempFile instanceof List) {
                localFileWriteSuccess = writeLocalFile((String) contents, (List) tempFile);
            } else if (contents instanceof InputStream) {
                localFileWriteSuccess = writeLocalFile((InputStream) contents, (String) tempFile);
            } else {
                throw new UnImplementedException("Unsupported type provided for either contents or tempFile arguments (or both)");
            }
            attempts += 1;
            if (attempts <= maxAttempts && Boolean.FALSE.equals(localFileWriteSuccess)) {
                try {
                    Thread.sleep(30 * 1000);
                } catch (InterruptedException e) {
                    log.error("Interruption encountered while sleeping between attempts");
                    return false;
                }
            }
        }

        return Boolean.TRUE.equals(localFileWriteSuccess);
    }

    /**
     * Looks for files prefixed by the given filePrefix in the current working directory.
     * @param filePrefix The beginning of the file name to search for.
     * @return A List of Paths to each of the matching files. If the returned list is empty, it means no files were
     * present in the current working directory which begin with the given filePrefix.
     */
    @Override
    public List<Path> findFilesFromNamePrefix(String filePrefix) throws IOException {
        List<File> rootDirs = List.of(new File(CommonUtils.generateTempFileRootPath()));
        final String regexPattern = filePrefix + "(.*)";
        List<Path> files = Lists.newArrayList();
        rootDirs.forEach(file -> {
            File[] filter = file.listFiles(pathname -> pathname.getName().matches(regexPattern));
            if (filter != null) Arrays.stream(filter).map(File::toPath).forEach(files::add);
        });
        return files;
    }

    /**
     * Iterates through a list of Paths and deletes each local file.
     * Uses the {@link #deleteLocalFile(Path)} method.
     *
     * @param inputFiles The list of local files to delete.
     */
    @Override
    public void deleteLocalFiles(List<Path> inputFiles) {
        if (!inputFiles.isEmpty()) {
            for (Path inputFile : inputFiles) deleteLocalFile(inputFile);
        } else {
            log.info("No local files found to delete.");
        }
    }
}
