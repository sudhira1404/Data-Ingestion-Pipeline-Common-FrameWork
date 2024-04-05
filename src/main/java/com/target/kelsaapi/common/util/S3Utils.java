package com.target.kelsaapi.common.util;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.target.kelsaapi.common.exceptions.AuthenticationException;
import com.target.kelsaapi.common.vo.s3.S3ObjectSummaryState;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

@Slf4j
public class S3Utils implements textFormatterInterface {

    public static List<String> getListOfFilesFromFolder( AmazonS3 s3Client,String bucket_name, String folderKey) throws AuthenticationException {

        ListObjectsRequest listObjectsRequest =
                new ListObjectsRequest()
                        .withBucketName(bucket_name)
                        .withPrefix(folderKey);

        List<String> files = new ArrayList<>();
        try {
            ObjectListing objects = s3Client.listObjects(listObjectsRequest);
            List<S3ObjectSummary> objSummaries = objects.getObjectSummaries();
            log.info(ANSI_GREEN + "list of object summary found:" + objSummaries.toString() + ANSI_RESET);
            for (S3ObjectSummary os : objSummaries) {
                log.info(ANSI_GREEN + "key found:" + os.getKey() + ANSI_RESET);
                files.add(os.getKey());
            }
        }
        catch (AmazonServiceException ase) {
            log.error("AmazonServiceException");
            log.error("Error Message:    " + ase.getMessage());
            log.error("HTTP Status Code: " + ase.getStatusCode());
            log.error("AWS Error Code:   " + ase.getErrorCode());
            log.error("Error Type:       " + ase.getErrorType());
            log.error("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            log.error("AmazonClientException,an internal error while trying to communicate with S3");
            throw new AuthenticationException(ace.getMessage(), ace.getCause());
        }

        return files;

    }

    public static List<S3ObjectSummary> getListOfFilesObjectsFromFolder( AmazonS3 s3Client,String bucket_name, String folderKey) throws AuthenticationException {

        ListObjectsRequest listObjectsRequest =
                new ListObjectsRequest()
                        .withBucketName(bucket_name)
                        .withPrefix(folderKey);
        List<S3ObjectSummary> objSummaries = new ArrayList<>();
        try {
            ObjectListing objects = s3Client.listObjects(listObjectsRequest);
            objSummaries = objects.getObjectSummaries();
        }
        catch (AmazonServiceException ase) {
            log.error("AmazonServiceException");
            log.error("Error Message:    " + ase.getMessage());
            log.error("HTTP Status Code: " + ase.getStatusCode());
            log.error("AWS Error Code:   " + ase.getErrorCode());
            log.error("Error Type:       " + ase.getErrorType());
            log.error("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            log.error("AmazonClientException,an internal error while trying to communicate with S3");
            throw new AuthenticationException(ace.getMessage(), ace.getCause());
        }

        log.info("list of object summary found:" + objSummaries.toString());

        return objSummaries;

    }

    public static List<String> getBulkListOfFilesObjectsFromFolder(AmazonS3 s3Client,String bucketName, String folderKey) {

        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(bucketName)
                .withPrefix(folderKey );
        List<String> keys = new ArrayList<>();
        try {
            ObjectListing objects = s3Client.listObjects(listObjectsRequest);
            for (; ; ) {
                List<S3ObjectSummary> summaries = objects.getObjectSummaries();
                if (summaries.size() < 1) {
                    break;
                }
                //summaries.forEach(s -> keys.add(s.getKey()));
                for (S3ObjectSummary objectSummary :
                        summaries) {
                    keys.add(objectSummary.getKey());
                    log.info(" - " + objectSummary.getKey() + "  " +
                            "(size = " + objectSummary.getSize() +
                            ")");
                }
                objects = s3Client.listNextBatchOfObjects(objects);
            }
        }
        catch (AmazonServiceException ase) {
            log.error("AmazonServiceException");
            log.error("Error Message:    " + ase.getMessage());
            log.error("HTTP Status Code: " + ase.getStatusCode());
            log.error("AWS Error Code:   " + ase.getErrorCode());
            log.error("Error Type:       " + ase.getErrorType());
            log.error("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            log.error("AmazonClientException,an internal error while trying to communicate with S3");
            log.error("Error Message: " + ace.getMessage());
        }
        return keys;
    }

    public static List<String> getBulkListOfFilesObjectsFromFolderUsingNextMarker(AmazonS3 s3Client,String bucketName, String folderKey) {

        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(bucketName)
                .withPrefix(folderKey);
        ObjectListing objectListing;
        List<String> keys = new ArrayList<>();
        try {
            do {
                objectListing = s3Client.listObjects(listObjectsRequest);
                for (S3ObjectSummary objectSummary :
                        objectListing.getObjectSummaries()) {
                    keys.add(objectSummary.getKey());
                    log.info(" - " + objectSummary.getKey() + "  " +
                            "(size = " + objectSummary.getSize() +
                            ")");
                }
                listObjectsRequest.setMarker(objectListing.getNextMarker());
            } while (objectListing.isTruncated());
        }
        catch (AmazonServiceException ase) {
            log.error("AmazonServiceException");
            log.error("Error Message:    " + ase.getMessage());
            log.error("HTTP Status Code: " + ase.getStatusCode());
            log.error("AWS Error Code:   " + ase.getErrorCode());
            log.error("Error Type:       " + ase.getErrorType());
            log.error("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            log.error("AmazonClientException,an internal error while trying to communicate with S3");
            log.error("Error Message: " + ace.getMessage());
        }
        return keys;

    }

    public static List<File> listFileNames( String path,List<File> listOfFiles ) {

        File root = new File( path );
        File[] list = root.listFiles();
        for ( File f : list ) {
            if ( f.isDirectory() ) {
                listFileNames( f.getAbsolutePath(),listOfFiles );
                //log.info( "ignoring Dir:" + f.getAbsoluteFile() );
            }
            else {
                if (f.isHidden()) {
                    //log.info("ignoring Hidden file:" + f);
                }else {
                    listOfFiles.add(f.getAbsoluteFile());
                }
            }
        }
        //log.info("List of files present in the local path:" + path + " after download " + " :\n " + listOfFiles);
        return listOfFiles;
    }

    public static Boolean filterFiles(String fileDate,Integer fileAge) throws ParseException {

        Boolean dropFileFlag=false;

        if (fileAge > 0) {
            LocalDate currDate = LocalDate.parse(java.time.LocalDate.now().toString());
            String dateFromFileAge = currDate.minusDays(fileAge).toString();
            SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
            Date fileModifiedDate = sdformat.parse(fileDate);
            Date fileAgeDate = sdformat.parse(dateFromFileAge);
            log.info("The fileModifiedDate is=" + sdformat.format(fileModifiedDate));
            log.info("The fileAgeDate is:=" + sdformat.format(fileAgeDate));
            if (fileModifiedDate.compareTo(fileAgeDate) >= 0) {
                log.info(String.format(ANSI_GREEN+"File modified date %s is greater or equal than the specified fileage date %s,so the file " +
                        " has not aged out and will be considered for download" + ANSI_RESET, sdformat.format(fileModifiedDate), sdformat.format(fileAgeDate)));
                dropFileFlag = false;
            } else if (fileModifiedDate.compareTo(fileAgeDate) < 0) {
                log.info(String.format(ANSI_YELLOW + "File modified date %s is less than the specified fileage date %s,so file " +
                        "has aged out and will not be considered for download", sdformat.format(fileModifiedDate), sdformat.format(fileAgeDate)) + ANSI_RESET);
                dropFileFlag = true;
            } else if (fileModifiedDate.compareTo(fileAgeDate) == 0) {
                log.info("Both dates are equal");
                dropFileFlag = true;
            }
        }
        return dropFileFlag;
    }

    public static List<String> diffCurPrevLoadedFiles(List curFilenames,List prevFilenames,Boolean compareWithPrevLoadedFilesCheck)
    {
        List<String> differences=curFilenames;

        if (compareWithPrevLoadedFilesCheck) {
            differences = new ArrayList<>(curFilenames);
            differences.removeAll(prevFilenames);
        }
        return differences;

    }


    public static List<S3ObjectSummary> diffCurPrevLoadedFiles(List<S3ObjectSummary> currFileObject,List prevFilenames,String reportType) {

        List<S3ObjectSummary> newObjectSummary = new ArrayList<>();

            for (S3ObjectSummary objectSummary : currFileObject) {

                Path path = Paths.get(objectSummary.getKey());
                String filename = path.getFileName().toString();

                if (prevFilenames.contains(filename)) {
                    log.info(String.format(ANSI_YELLOW + "File %s has an entry in the PGDB(mdf_s3_processed_filenames) for the reporttype %s,indicating its already been processed," +
                            "so this file will not be downloaded.Delete the file from PGDB to reprocess",filename,reportType) + ANSI_RESET);
                } else {
                    log.info(String.format("File %s does not have an entry the the PGDB(mdf_s3_processed_filenames) for the reporttype %s," +
                            "indicating its not already been processed,so this file will be downloaded",filename,reportType));
                    newObjectSummary.add(objectSummary);
                }
            }

        return newObjectSummary;

    }

    public static List<S3ObjectSummary> curPrevLoadedFileObjectsEtagCheck(List<S3ObjectSummaryState> prevFileObject, List<S3ObjectSummary> currFileObject, String reportType) {

        S3ObjectSummaryState prevFileObjectSingle = null;
        List<S3ObjectSummary> newObjectSummary = new ArrayList<>();
        Boolean fileFound= false;

        for (S3ObjectSummary objectSummary : currFileObject) {
            log.info("Table mdf_s3_objectsummary row count size " + prevFileObject.size());
            if (prevFileObject.size() > 0) {
                for (int i = 0; i < prevFileObject.size(); i++) {
                    prevFileObjectSingle = prevFileObject.get(i);
//                    log.info("prev object etag and file " + prevFileObjectSingle.getEtag() + " " + prevFileObjectSingle.getKey() );
//                    log.info("Curr etag and key" + " " + objectSummary.getETag() + " " +objectSummary.getKey());
                    if (prevFileObjectSingle.getEtag().trim().equals(objectSummary.getETag().trim()) && prevFileObjectSingle.getKey().trim().equals(objectSummary.getKey().trim())) {
                        log.info(String.format(ANSI_YELLOW + "File %s with etag %s has an entry in the PGDB(mdf_s3_objectsummary) for the reporttype %s,indicating the same file is already been processed" +
                                " ,so this file will not be downloaded.Delete the file from PGDB to reprocess", prevFileObjectSingle.getKey(), prevFileObjectSingle.getEtag(), reportType) + ANSI_RESET);
                        fileFound=true;
                        break;
                    }
                }
                if (!fileFound) {
                    log.info(String.format(ANSI_GREEN +"File %s with etag %s does not have an entry the the PGDB(mdf_s3_objectsummary) for the reporttype %s," +
                            "indicating its not already been processed,so this file will be downloaded", prevFileObjectSingle.getKey(), prevFileObjectSingle.getEtag(), reportType) + ANSI_RESET);
                    newObjectSummary.add(objectSummary);
                }

            } else {
                log.info(String.format(ANSI_GREEN +"File %s with etag %s does not have an entry the the PGDB(mdf_s3_objectsummary) for the reporttype %s," +
                        "indicating its not already been processed,so this file will be downloaded", objectSummary.getKey(), objectSummary.getETag(), reportType) + ANSI_RESET);
                //newObjectSummary = currFileObject;
                newObjectSummary.add(objectSummary);
            }

            }


        return newObjectSummary;
    }

    public static long getObjectSize(AmazonS3 s3Client,String bucketName,String objectKey) {

        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName,objectKey);
        S3Object s3Object = s3Client.getObject(getObjectRequest);
        return s3Object.getObjectMetadata().getContentLength();
    }

    public static String getUtcDateTime() {

        OffsetDateTime dateTime = OffsetDateTime.now(ZoneOffset.UTC);
        String strDateTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(dateTime);

        return strDateTime;
    }

    public static String getUtcDate() {

        OffsetDateTime dateTime = OffsetDateTime.now(ZoneOffset.UTC);
        return DateTimeFormatter.ofPattern("yyyy-MM-dd").format(dateTime);
    }

    public static int getUtcHour() {

        OffsetDateTime dateTime = OffsetDateTime.now(ZoneOffset.UTC);
        return Integer.parseInt(DateTimeFormatter.ofPattern("HH").format(dateTime));
    }

    public static Date stringToDate(String dateInString) {

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

        Date date = new Date();
        try {
            date = dateFormat.parse(dateInString);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return date;
    }


    public static String dateToString(Date date) {

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String strDate = dateFormat.format(date);
        return strDate;
    }


    public static java.sql.Date dateToSqlDate(Date date) {

        java.sql.Date ts = new java.sql.Date(date.getTime());
        return ts;
    }

    public static Timestamp dateToSqlTime(Date date) {

        Timestamp ts=new Timestamp(date.getTime());
        return ts;
    }

    public static List<String> getListOfFilenames(Connection connect, String sourceReportType) throws SQLException {

        List<String> listOfFilenames = new ArrayList<String>();
        String sql = String.format("select * from mdf_s3_processed_filenames where source_report_type = '%s'",sourceReportType);
        System.out.println(sql);
        try (Connection conn = connect;
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while(rs.next())
            {
                listOfFilenames.add(rs.getString("filenames"));
            }

        } catch (SQLException ex) {
            log.info(ex.getMessage());
        }

        return listOfFilenames;
    }

    public static void insertFilenamesToDb(Connection connect, String sourceReportType,String filename) {

        //String sql = String.format("delete from mdf_s3_processed_filenames where source_report_type = '%s' and filenames='%s';insert into mdf_s3_processed_filenames(source_report_type,filenames) values('%s','%s')",sourceReportType,filename,sourceReportType,filename);
        String sql = String.format("insert into mdf_s3_processed_filenames(source_report_type,filenames) values('%s','%s')",sourceReportType,filename);
        log.info((sql));
        try (Connection conn = connect;
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
        } catch (SQLException ex) {
            log.info(ex.getMessage());
        }

    }

}
