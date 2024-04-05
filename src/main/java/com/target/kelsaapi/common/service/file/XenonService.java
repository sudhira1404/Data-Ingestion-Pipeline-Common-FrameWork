package com.target.kelsaapi.common.service.file;

import com.target.kelsaapi.common.exceptions.NotFoundException;
import com.target.kelsaapi.common.exceptions.ReaderException;
import com.target.kelsaapi.common.exceptions.WriterException;

import java.util.List;

public interface XenonService {

    Boolean transferFile(String filePath, String tempFile, Boolean overwrite, Boolean mkdirs, Boolean append);

    Boolean deleteFile(String filePath) throws  WriterException;

    Boolean deleteFolder(String folderPath) throws WriterException, NotFoundException;

    Boolean isFileExists(String filePath);

    Boolean isFolderExists(String folderPath) throws NotFoundException;

    List<String> readFile(String path) throws ReaderException;

    List<String> readFolder(String folderPath) throws ReaderException;

    Boolean createFolder(String folderPath) throws WriterException;

    String getXenonNode();

}
