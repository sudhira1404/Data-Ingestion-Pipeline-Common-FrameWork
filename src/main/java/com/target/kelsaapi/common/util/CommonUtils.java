package com.target.kelsaapi.common.util;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.ConfigurationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.lang.Nullable;
import org.springframework.util.StopWatch;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

/**
 * Common utility methods
 *
 * @since 1.0
 */
@Slf4j
public class CommonUtils {
    public static Boolean checkIfEmpty(String name, String value){
        Boolean isEmpty = false;
        log.info("{} has the value {}",name,value);
        if(value.isEmpty()){
            log.error("{} is missing",name);
            isEmpty = true;
        }
        return isEmpty;
    }

    public static HttpHeaders createHeaders(Map<String,String> headersMap){
        final HttpHeaders headers = new HttpHeaders();
        for (Map.Entry<String, String> header : headersMap.entrySet()) {
            headers.set(header.getKey(), header.getValue());
        }
        return headers;
    }


    public static void timerSplit(StopWatch stopWatch, String message) {
        if (stopWatch.isRunning()) {
            stopWatch.stop();
        }
        stopWatch.start(message);
    }


    public static String prettyPrintStopWatchSeconds(StopWatch stopWatch) {
        NumberFormat nf = NumberFormat.getNumberInstance();
        nf.setMinimumIntegerDigits(3);
        nf.setGroupingUsed(false);


        NumberFormat pf = NumberFormat.getPercentInstance();
        pf.setMinimumIntegerDigits(3);
        pf.setGroupingUsed(false);

        StringBuilder sb = new StringBuilder();
        sb.append("StopWatch '").append(stopWatch.getId()).append("': running time = ")
                .append(nf.format(stopWatch.getTotalTimeSeconds())).append(" s");
        sb.append('\n');
        sb.append("---------------------------------------------\n");
        sb.append("s          %     Task name\n");
        sb.append("---------------------------------------------\n");

        for (StopWatch.TaskInfo task : stopWatch.getTaskInfo()) {
            sb.append(nf.format(task.getTimeSeconds())).append("  ");
            sb.append(pf.format(task.getTimeSeconds() / stopWatch.getTotalTimeSeconds())).append("  ");
            sb.append(task.getTaskName()).append("\n");
        }
        return sb.toString();
    }
    /**
     * Split date range into list of dates
     * @param startDate
     * @param endDate
     * @return List of dates
     */
    public static List<String> splitDateRange(String startDate, String endDate) throws ParseException {
        List<String> dateList = new ArrayList<>();
        Calendar start = new GregorianCalendar();
        Calendar end = new GregorianCalendar();
        java.util.Date date1 = new SimpleDateFormat("yyyy-MM-dd").parse(startDate);
        java.util.Date date2 = new SimpleDateFormat("yyyy-MM-dd").parse(endDate);
        start.setTime(date1);
        end.setTime(date2);
        for (Calendar startVar = start; startVar.before(end)|| startVar.equals(end);startVar.add(Calendar.DATE,1)) {
            dateList.add(new SimpleDateFormat("yyyy-MM-dd").format(startVar.getTime()));
        }
        return dateList;
    }

    /**
     * Create Filepath based on source
     *
     * @param source One of the supported source types from {@link ApplicationConstants.Sources}
     * @param targetPath The root path to the directory where the file will land
     * @param startDate The start date in yyyy-MM-dd format
     * @param endDate The end date in yyyy-MM-dd format
     * @param reportType The type of report requested
     * @return filepath string
     * @throws ConfigurationException If the source is not configured properly. Check {@link # generateFileExtension(ApplicationConstants.Sources)}
     */
    public static String generateFilePath(ApplicationConstants.Sources source, String targetPath, String startDate,
                                          String endDate, String reportType) throws ConfigurationException {
        String fileExt = generateFileExtension(source, reportType);
        return targetPath + source.toString().toLowerCase() + "-" + reportType + "-report-" + startDate + "_" + endDate + fileExt;
    }

    /**
     * Create file extension based on source
     *
     * @param source One of the supported source types from {@link ApplicationConstants.Sources}
     * @param reportTypeName The name of the report type for this source
     * @return the file extension
     * @throws ConfigurationException If the source is not part of the {@link ApplicationConstants.Sources} enum
     */
    public static String generateFileExtension(ApplicationConstants.Sources source, String reportTypeName) throws ConfigurationException {
        String name = reportTypeName.toUpperCase();
        String fileExt;
        switch (source) {
            case CAMPAIGN_MANAGER_360:
                fileExt = ApplicationConstants.CampaignManager360ReportTypes.valueOf(name).getFileExtension();
                break;
            case FACEBOOK:
                fileExt = ApplicationConstants.FacebookReportTypes.valueOf(name).getFileExtension();
                break;
            case GAM:
                fileExt = ApplicationConstants.GamReportTypes.valueOf(name).getFileExtension();
                break;
            case PINTEREST:
                fileExt = ApplicationConstants.PinterestReportTypes.valueOf(name).getFileExtension();
                break;
            case SNAPCHAT:
                fileExt = ApplicationConstants.SnapChatReportTypes.valueOf(name).getFileExtension();
                break;
            case TRADEDESK:
                fileExt = ApplicationConstants.TradedeskReportTypes.valueOf(name).getFileExtension();
                break;
            case TRADEDESKAPI:
                fileExt = ApplicationConstants.TradedeskApiReportTypes.valueOf(name).getFileExtension();
                break;
            case XANDR:
                fileExt = ApplicationConstants.XandrReportTypes.valueOf(name).getFileExtension();
                break;
            case INDEXEXCHANGE:
                fileExt = ApplicationConstants.IndexExchangeReportTypes.valueOf(name).getFileExtension();
                break;
            case SALESFORCE:
                fileExt = ApplicationConstants.SalesforceReportTypes.valueOf(name).getFileExtension();
                break;
            case CRITEO:
                fileExt = ApplicationConstants.SftpReportTypes.valueOf(name).getFileExtension();
                break;
            default:
                throw new ConfigurationException("Requested source not currently supported: " + source);
        }
        return fileExt;
    }

    /**
     * Converts the matching ReportType enum based on the specified source to a Stream
     * @param source The source as specified in {@link ApplicationConstants.Sources}
     * @return A Stream from the matching ReportType enum for that specified source
     * @throws ConfigurationException The specified source is not supported.
     */
    public static Stream<?> streamReportTypes(ApplicationConstants.Sources source) throws ConfigurationException {
        Stream<?> valueStream;
        switch (source) {
            case CAMPAIGN_MANAGER_360:
                valueStream = Arrays.stream(ApplicationConstants.CampaignManager360ReportTypes.values());
                break;
            case FACEBOOK:
                valueStream = Arrays.stream(ApplicationConstants.FacebookReportTypes.values());
                break;
            case GAM:
                valueStream = Arrays.stream(ApplicationConstants.GamReportTypes.values());
                break;
            case PINTEREST:
                valueStream = Arrays.stream(ApplicationConstants.PinterestReportTypes.values());
                break;
            case SNAPCHAT:
                valueStream = Arrays.stream(ApplicationConstants.SnapChatReportTypes.values());
                break;
            case TRADEDESK:
                valueStream = Arrays.stream(ApplicationConstants.TradedeskReportTypes.values());
                break;
            case TRADEDESKAPI:
                valueStream = Arrays.stream(ApplicationConstants.TradedeskApiReportTypes.values());
                break;
            case XANDR:
                valueStream = Arrays.stream(ApplicationConstants.XandrReportTypes.values());
                break;
            case INDEXEXCHANGE:
                valueStream = Arrays.stream(ApplicationConstants.IndexExchangeReportTypes.values());
                break;
            case SALESFORCE:
                valueStream = Arrays.stream(ApplicationConstants.SalesforceReportTypes.values());
                break;
            case CRITEO:
                valueStream = Arrays.stream(ApplicationConstants.SftpReportTypes.values());
                break;
            default:
                throw new ConfigurationException("Source " + source.toString().toLowerCase(Locale.ROOT) + " not supported");
        }
        return valueStream;
    }

    public static BackOff startBackOff() {
        int DEFAULT_INITIAL_INTERVAL = 10 * 1000; // 10 second initial retry
        int DEFAULT_MAX_RETRY_INTERVAL = 10 * 60 * 1000; // 10 minute maximum retry
        int DEFAULT_TOTAL_TIME_TO_WAIT = 120 * 60 * 1000; // 2 hour total retry
        return startBackOff(DEFAULT_INITIAL_INTERVAL, DEFAULT_MAX_RETRY_INTERVAL, DEFAULT_TOTAL_TIME_TO_WAIT);
    }

    public static BackOff startBackOff(int initialWaitTimeBetweenRetries, int maxWaitTimeBetweenRetries, int totalTimeToWait) {
        return new ExponentialBackOff.Builder()
            .setInitialIntervalMillis(initialWaitTimeBetweenRetries)
            .setMaxIntervalMillis(maxWaitTimeBetweenRetries)
            .setMaxElapsedTimeMillis(totalTimeToWait)
            .build();

    }

    public static String generateTempFilePathAndName(String pipelineRunId, ApplicationConstants.Sources source, @Nullable String reportType) throws ConfigurationException, IOException {
        String tempFilePath;
        if (source.equals(ApplicationConstants.Sources.S3)) {
            tempFilePath = generateTempFileRootPath() + pipelineRunId;
        } else {
            assert(reportType!=null);
            tempFilePath = generateTempFileRootPath() + pipelineRunId + CommonUtils.generateFileExtension(source, reportType);
        }
        log.debug("Generated temp file path is: {}", tempFilePath);
        return tempFilePath;
    }

    public static String generateTempFileRootPath() throws IOException {
        String cwd = System.getProperty("user.dir");
        String env = Objects.requireNonNullElse(System.getenv("CLOUD_ENVIRONMENT"),"local");
        log.debug("Runtime environment to configure temp root folder path in: {}",env);
        String tempPath;
        if (env.equals("local")) {
            tempPath = cwd + File.separator + ApplicationConstants.TEMP_FOLDER_ROOT_NAME + File.separator;
        } else {
            tempPath = File.separator + ApplicationConstants.TEMP_FOLDER_ROOT_NAME + File.separator;
        }
        //Test to ensure directory exists and is writable
        File tempDir = new File(tempPath);
        if (tempDir.exists() && tempDir.isDirectory()) {
            log.debug("Generated temp root folder is: {}",tempPath);
            return tempPath;
        } else {
            throw new IOException("Unable to write to directory: " + tempPath);
        }
    }

    public static void validateDateFormat(String date) throws ParseException {
        String dateFormat = "yyyy-MM-dd";
        DateFormat sdf = new SimpleDateFormat(dateFormat);
        sdf.setLenient(false);
        sdf.parse(date);
    }
}
