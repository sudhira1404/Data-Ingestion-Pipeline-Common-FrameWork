package com.target.kelsaapi.common.service.file;

import com.target.kelsaapi.common.exceptions.UnImplementedException;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class LocalFileWriterServiceImplTest {

    String testString;

    List<String> testListString = Lists.newArrayList();

    InputStream testInputStream;

    LocalFileWriterService localFileWriterService;

    String testFile1;

    String testFile2;

    String testFile3;

    String testFile4;

    String dataFile1;

    String dataFile2;

    @BeforeAll
    void setUp() {
        this.testString = "This is a test string.";

        this.testListString.add("Test String 1");
        this.testListString.add("Test String 2");
        this.testListString.add("Test String 3");

        this.testInputStream = IOUtils.toInputStream(this.testString, StandardCharsets.UTF_8);

        this.localFileWriterService = new LocalFileWriterServiceImpl();

        this.testFile1 = "testFile1.txt.gz";
        this.testFile2 = "testFile2.txt.gz";
        this.testFile3 = "testFile3.tar.gz";
        this.testFile4 = "testFile4.txt.gz";
        this.dataFile1 = "dataFile1.dat";
        this.dataFile2 = "dataFile2.dat";
    }

    @AfterAll
    void cleanupFiles() throws IOException {
        File firstTestFile = new File(testFile1);
        if (firstTestFile.exists()) FileUtils.delete(firstTestFile);

        File secondTestFile = new File(testFile2);
        if (secondTestFile.exists()) FileUtils.delete(secondTestFile);

        File thirdTestFile = new File(testFile3);
        if (thirdTestFile.exists()) FileUtils.delete(thirdTestFile);

        File fourthTestFile = new File(testFile4);
        if (fourthTestFile.exists()) FileUtils.delete(fourthTestFile);
    }

    @Test
    public void testLocalFileWriteWithString() {
        Assertions.assertTrue(localFileWriterService.writeLocalFile(testString, testFile1));
    }

    @Test
    public void testLocalFileWriteWithList() {
        Assertions.assertTrue(localFileWriterService.writeLocalFile(testListString, testFile2));
    }

    @Test
    public void testLocalGzipTarFileWrite() {

        Assertions.assertTrue(localFileWriterService.writeLocalFile(testListString, dataFile1, false));
        Assertions.assertTrue(localFileWriterService.writeLocalFile(testListString, dataFile2, false));


        Assertions.assertTrue(localFileWriterService.writeLocalFile(testFile3, List.of(Path.of(dataFile1), Path.of(dataFile2))));

    }

    @Test
    public void testLocalFileWriteWithInputStream() {
        Assertions.assertTrue(localFileWriterService.writeLocalFile(testInputStream, testFile4));
    }

    @Test
    @AfterAll
    public void testRetryableLocalWrite() throws UnImplementedException, IOException {
        cleanupFiles();
        Assertions.assertTrue(localFileWriterService.retryableLocalWrite(testString, testFile1));
        Assertions.assertTrue(localFileWriterService.retryableLocalWrite(testListString, testFile2));
        Assertions.assertTrue(localFileWriterService.writeLocalFile(testListString, dataFile1, false));
        Assertions.assertTrue(localFileWriterService.writeLocalFile(testListString, dataFile2, false));
        Assertions.assertTrue(localFileWriterService.retryableLocalWrite(testFile3, List.of(Path.of(dataFile1), Path.of(dataFile2))));
        Assertions.assertTrue(localFileWriterService.retryableLocalWrite(testInputStream, testFile4));
    }
}