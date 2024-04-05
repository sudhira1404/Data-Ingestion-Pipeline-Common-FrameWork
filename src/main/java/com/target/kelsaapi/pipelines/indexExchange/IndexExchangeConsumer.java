package com.target.kelsaapi.pipelines.indexExchange;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.AuthenticationException;
import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.common.exceptions.HttpException;
import com.target.kelsaapi.common.service.indexExchange.IndexExchangeService;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.vo.Oauth;
import com.target.kelsaapi.common.vo.indexexchange.ReportsList;
import com.target.kelsaapi.pipelines.EndPointConsumer;
import com.target.kelsaapi.pipelines.EndPointConsumerInterface;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

@Slf4j
public class IndexExchangeConsumer extends EndPointConsumer implements EndPointConsumerInterface {

    private final IndexExchangeService indexExchangeService;


    final private Oauth oauth;

    public IndexExchangeConsumer(ApplicationContext context, String pipelineRunId) throws ConfigurationException {
        super(context, pipelineRunId);
        this.indexExchangeService = context.getBean(IndexExchangeService.class);
        PipelineConfig.IndexExchange indexExchangeConfig = pipelineConfig.getApiconfig().getSource().getIndexExchange();
        this.oauth = new Oauth(context, indexExchangeConfig.getAuthentication());
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
    public void execute(String startDate, String endDate, String targetFile, String reportType, StopWatch stopWatch) throws RuntimeException, SQLException, IOException, AuthenticationException {
        runPipeline(startDate, targetFile, getReportType(reportType), stopWatch);
    }

    private void runPipeline(String startDate, String targetFile, ApplicationConstants.IndexExchangeReportTypes reportType,
                             StopWatch stopWatch)
            throws RuntimeException, HttpException, JsonProcessingException {

        //Begin the timer
        CommonUtils.timerSplit(stopWatch, "Initialization");
        try {
            //Initialize header
            Map<String, String> headersMap = indexExchangeService.setHeaderMap(oauth);
            CommonUtils.timerSplit(stopWatch, "Ingest from API");
            log.info("Index Exchange: working on report {}",  reportType.name());
            log.debug(oauth.getOAuthToken());

            ArrayList<ReportsList> reportsList = indexExchangeService.listReports(headersMap, reportType.getReportId());

            sortReportsList(reportsList);
            for (ReportsList list : reportsList) {
                System.out.println(list);
            }
            // get only latest report File ID from the return list
            // only latest day report is enough, as everyday report will have data for the past 14 days
            int latestReportFileId = reportsList.get(0).getFileID(); //12605179
            log.info("Index exchange: {} report fileID: {} to download", reportType.name(), latestReportFileId);
            //Fetch downloadable file contents
            String reportContents = indexExchangeService.downloadReport(headersMap, latestReportFileId);

            String tempFile = CommonUtils.generateTempFilePathAndName(pipelineRunId, ApplicationConstants.Sources.INDEXEXCHANGE, reportType.name());
            log.debug("local temp file name: "+tempFile);

            Boolean localWriteSuccessful = localFileWriterService.writeLocalFile(reportContents, tempFile, true, true);
            if (Boolean.FALSE.equals(localWriteSuccessful)) {
                throw new IOException("write attempt to local filesystem failed for " + tempFile);
            }

            CommonUtils.timerSplit(stopWatch, "Write to HDFS");
            log.info("Attempting to write downloaded Index exchange: {} data to HDFS", reportType.name());
            Boolean finalWriteSuccessful = writerService.writeToHDFS(targetFile,3, tempFile, true);
            if (Boolean.FALSE.equals(finalWriteSuccessful)) {
                throw new IOException("All write attempts to HDFS failed for " + targetFile);
            }

        } catch (Exception e) {
            log.error("Index Exchange - Exception thrown while retrieving results", e);
            throw new RuntimeException(e.getMessage(), e.getCause());
        }

    }


    private ApplicationConstants.IndexExchangeReportTypes getReportType(String reportType) {
        return ApplicationConstants.IndexExchangeReportTypes.valueOf(reportType.toUpperCase());
    }

    private void sortReportsList( ArrayList<ReportsList> reportsList){

        reportsList.sort((a, b) -> {

            String valA = a.getCreatedAt();
            String valB = b.getCreatedAt();

            return -valA.compareTo(valB);
        });
    }
}