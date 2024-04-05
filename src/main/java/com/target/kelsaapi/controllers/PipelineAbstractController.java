package com.target.kelsaapi.controllers;

import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.common.exceptions.ResourceNotFoundException;
import com.target.kelsaapi.common.service.listener.PipelineRunnerListener;
import com.target.kelsaapi.common.service.postgres.pipelinerunstate.PipelineRunStateService;
import com.target.kelsaapi.common.service.postgres.s3.S3DbParamStateService;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.validators.ControllerValidator;
import com.target.kelsaapi.common.vo.pipeline.response.PipelineRunStatusResponse;
import com.target.kelsaapi.common.vo.pipeline.state.PipelineRunState;
import com.target.kelsaapi.common.vo.s3.S3DbParam;
import com.target.kelsaapi.common.vo.s3.S3DbParamState;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

@Slf4j
public abstract class PipelineAbstractController {

    protected final ControllerValidator validator;

    protected final PipelineRunStateService runStateService;

    protected final S3DbParamStateService s3DbParamStateService;

    protected final PipelineRunnerListener pipelineRunnerListenerService;

    public PipelineAbstractController(ControllerValidator validator,
                                      PipelineRunStateService runStateService,
                                      S3DbParamStateService s3DbParamStateService,
                                      PipelineRunnerListener pipelineRunnerListener) {
        this.validator = validator;
        this.runStateService = runStateService;
        this.s3DbParamStateService = s3DbParamStateService;
        this.pipelineRunnerListenerService = pipelineRunnerListener;
    }

    protected ApplicationConstants.Sources validateSources(String source) throws ResponseStatusException {
        Boolean validSource = validator.validateSources(source);

        if (validSource) {
            log.info("streamProcess:: {}", source);
            return ApplicationConstants.Sources.valueOf(source.toUpperCase());
        } else {
            String sourceTypes = Arrays.toString(ApplicationConstants.Sources.values()).toLowerCase();
            String errorMessage = "Invalid source. Must be one of these types: " + sourceTypes;
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,errorMessage);
        }
    }


    protected List<String> validateDates(String startDate, String endDate) throws ResponseStatusException, ParseException {
        Boolean validDates = validator.validateDateFormats(startDate, endDate);

        if (validDates) {
            log.info("startDate:: {}", startDate);
            log.info("endDate:: {}", endDate);
            return dateRangeSplitter(startDate, endDate);
        } else {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Dates incorrectly formatted. Please give dates in yyyy-MM-dd format.");
        }
    }

    protected void validateMembers(String memberships) throws ResponseStatusException {
        Boolean validAuth = validator.validateMembership(memberships);

        if (validAuth) {
            log.info("Successfully authenticated request.");
        } else {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Not authorized.");
        }
    }

    protected void validateFilePath(String filePath) throws ResponseStatusException {
        Boolean validFilePath = validator.validatePath(filePath);

        if (validFilePath) {
            log.info("Input target path is valid");
        } else {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Please provide a valid hdfs path");
        }
    }

    protected List<String> validateReportTypes(String reportType, ApplicationConstants.Sources source)
            throws ResponseStatusException, ConfigurationException {
        if (reportType == null || reportType.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "No report_type specified");
        }
        List<String> reportTypes = reportTypeSplitter(reportType);
        for (String rt : reportTypes) {
            if (validator.validateReportTypes(source, rt)) {
                log.info("Valid report type:: {}", rt);
            } else {
                String errorMessage = "Invalid report type specified: " + rt + " for source: " + source;
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, errorMessage);
            }
        }
        return reportTypes;
    }

    protected List<String> dateRangeSplitter(String startDate, String endDate) throws ParseException {
        List<String> datesList = CommonUtils.splitDateRange(startDate, endDate);
        log.info("Dates requested are: {}",datesList);
        return datesList;
    }

    protected List<String> reportTypeSplitter(String reportType) {
        List<String> reportTypes = Arrays.asList(StringUtils.split(reportType, ","));
        log.info("Report types requested are: {}", reportTypes);
        return reportTypes;
    }

    protected PipelineRunStatusResponse getPipelineState(String memberships, String pipelineId) {
        //Validate Memberships
        validateMembers(memberships);

        log.info("Getting report run status for :" + pipelineId);
        PipelineRunState runState;
        try {
            runState = runStateService.findById(pipelineId);
        } catch (ResourceNotFoundException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND,e.getMessage());
        }
        return new PipelineRunStatusResponse(runState);
    }

    protected void asyncRequest(PipelineRunState runState,
                                List<PipelineRunStatusResponse> pipelineIds) throws ResponseStatusException {
        try {
            //Saves the requested pipeline run state to Postgres
            runStateService.save(runState);
            //Publishes the requested pipeline run state to async queue
            pipelineRunnerListenerService.runPipeline(runState);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        pipelineIds.add(new PipelineRunStatusResponse(runState));

        //return new PipelineRunStatusResponse(runState);
        log.info("Pipeline is " + new PipelineRunStatusResponse((runState)));
    }


    protected S3DbParam getS3DbParam(String reportType) {

        String level;
        Boolean dirDownload;
        Boolean xferMgrSingleFileDownload;
        Boolean s3PrefixSqlFunction;
        Boolean compareWithPrevLoadedFilesCheckByCurrentDate;
        Boolean compareWithPrevLoadedFilesCheck;
        Boolean abortNoFileFound;
        Integer fileAge;
        String s3PrefixPath;
        Boolean splitFile;
        Boolean splitFileCompress;

        S3DbParamState s3DbParamState = s3DbParamStateService.getS3DbParamState(reportType);

        try {
            level = s3DbParamState.getSourceReportType();
        } catch (NullPointerException e) {
            level = "";
        }
        if (level.isEmpty()) {
            return new S3DbParam("", false, false,false,false,false,false,0,"",false,false);
        } else {
            s3PrefixPath = s3DbParamState.getPrefix();
            dirDownload = s3DbParamState.getDirDownload();
            xferMgrSingleFileDownload = s3DbParamState.getXferMgrSingleFileDownload();
            s3PrefixSqlFunction = s3DbParamState.getS3PrefixSqlFunction();
            compareWithPrevLoadedFilesCheckByCurrentDate = s3DbParamState.getCompareWithPrevLoadedFilesCheckByCurrentDate();
            compareWithPrevLoadedFilesCheck = s3DbParamState.getCompareWithPrevLoadedFilesCheck();
            abortNoFileFound = s3DbParamState.getAbortNoFileFound();
            fileAge = s3DbParamState.getFileAge();
            splitFile =  s3DbParamState.getSplitFile();
            splitFileCompress = s3DbParamState.getSplitFileCompress();

            return new S3DbParam(s3PrefixPath, dirDownload, xferMgrSingleFileDownload,s3PrefixSqlFunction,compareWithPrevLoadedFilesCheckByCurrentDate,compareWithPrevLoadedFilesCheck,abortNoFileFound,fileAge,level,splitFile,splitFileCompress);
        }

    }

}
