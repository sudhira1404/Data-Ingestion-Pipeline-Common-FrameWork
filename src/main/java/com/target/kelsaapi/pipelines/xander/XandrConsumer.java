package com.target.kelsaapi.pipelines.xander;

import com.fasterxml.jackson.databind.JsonNode;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.common.service.xander.XandrService;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.vo.Oauth;
import com.target.kelsaapi.pipelines.EndPointConsumer;
import com.target.kelsaapi.pipelines.EndPointConsumerInterface;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;


@Slf4j
public class XandrConsumer extends EndPointConsumer implements EndPointConsumerInterface {

    private final XandrService xandrService;

    private final PipelineConfig.Xandr xandrConfig;

    final private Oauth oauth;

    public XandrConsumer(ApplicationContext context, String pipelineRunId) throws ConfigurationException {
        super(context, pipelineRunId);
        this.xandrService = context.getBean(XandrService.class);
        this.xandrConfig = pipelineConfig.getApiconfig().getSource().getXandr();
        this.oauth = new Oauth(context, xandrConfig.getAuthentication());
    }


    /**
     * Executes a pipeline run.
     *
     * @param startDate  The start date in yyyy-MM-dd format.
     * @param endDate    The end date in yyyy-MM-dd format.
     * @param targetFile The full file path to where data should be landed.
     * @param reportType The report type to execute.
     * @param stopWatch  The initialized StopWatch object.
     * @throws RuntimeException A catch-all for any exception raised during the run of this pipeline.
     */
    @Override
    public void execute(String startDate, String endDate, String targetFile, String reportType, StopWatch stopWatch)
            throws RuntimeException {
        runPipeline(startDate, targetFile, getReportType(reportType), stopWatch);
    }

    private void runPipeline(String startDate, String targetFile, ApplicationConstants.XandrReportTypes reportType,
                             StopWatch stopWatch)
            throws RuntimeException {
        //Begin the timer
        CommonUtils.timerSplit(stopWatch, "Initialization");

        try {
            //Initialize header
            Map<String, String> headersMap = xandrService.setHeaderMap(oauth);
            CommonUtils.timerSplit(stopWatch, "Ingest from API");
            log.info("Xandr - working on report {}", xandrConfig.report);

            JsonNode bodyMap = xandrService.setBodyMap(xandrConfig.report, reportType.name());
            log.info("bodyMap Value: {}", bodyMap.toPrettyString());
            String report_id = xandrService.requestReport(headersMap, bodyMap, xandrConfig.baseUrl, xandrConfig.reportEndPoint);
//            String report_status = xandrService.getReportStatus(headersMap, xandrConfig.baseUrl, xandrConfig.reportStatusEndPoint, report_id);

            String report_status = null;
            int minutes = 0;
            do {
                report_status = xandrService.getReportStatus(headersMap, xandrConfig.baseUrl, xandrConfig.reportEndPoint, report_id);
                try {
                    Thread.sleep(60 * 1000); //sleep for 60 seconds
                    minutes = minutes + 1;
                } catch (InterruptedException ie) {
                    log.error("Xandr - Exception thrown while tried to Sleep", ie);
                    Thread.currentThread().interrupt();
                }
            } while (!report_status.equalsIgnoreCase("ready") & minutes < 15);


            if (!report_status.equalsIgnoreCase("ready")) {
                log.error("Xandr - TimeOut error.. waited for 15 minutes still the report is not ready. Report id: "+report_id);
                throw new TimeoutException("Xandr - api Time out exception while waiting for the report: "+report_id);
            }

            //Fetch downloadable file contents
            headersMap.remove("Content-Type");
            String reportContents = xandrService.downloadReport(headersMap, xandrConfig.baseUrl, xandrConfig.reportDownloadEndPoint, report_id);
            String tempFile = CommonUtils.generateTempFilePathAndName(pipelineRunId, ApplicationConstants.Sources.XANDR, reportType.name());
            log.debug("Local tempfile name: "+tempFile);
            Boolean localWriteSuccessful = localFileWriterService.writeLocalFile(reportContents, tempFile, true, true);
            if (Boolean.FALSE.equals(localWriteSuccessful)) {
                throw new IOException("write attempt to local filesystem failed for " + tempFile);
            }

            CommonUtils.timerSplit(stopWatch, "Write to HDFS");
            log.info("Attempting to write downloaded Xandr: {} data to HDFS", reportType.name());
            Boolean finalWriteSuccessful = writerService.writeToHDFS(targetFile,3, tempFile, true);
            if (Boolean.FALSE.equals(finalWriteSuccessful)) {
                throw new IOException("All write attempts to HDFS failed for " + targetFile);
            }

        } catch (Exception e) {
            log.error("Xandr - Exception thrown while retrieving results", e);
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    private ApplicationConstants.XandrReportTypes getReportType(String reportType) {
        return ApplicationConstants.XandrReportTypes.valueOf(reportType.toUpperCase());
    }

}
