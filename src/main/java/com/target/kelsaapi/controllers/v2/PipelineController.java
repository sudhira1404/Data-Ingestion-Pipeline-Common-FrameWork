package com.target.kelsaapi.controllers.v2;

import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.constants.ApplicationConstants.PipelineStates;
import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.common.service.listener.PipelineRunnerListener;
import com.target.kelsaapi.common.service.postgres.pipelinerunstate.PipelineRunStateService;
import com.target.kelsaapi.common.service.postgres.s3.S3DbParamStateService;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.validators.ControllerValidator;
import com.target.kelsaapi.common.vo.pipeline.request.PipelineRunRequest;
import com.target.kelsaapi.common.vo.pipeline.response.PipelineRunStatusResponse;
import com.target.kelsaapi.common.vo.pipeline.response.PipelineRunStatusResponses;
import com.target.kelsaapi.common.vo.pipeline.state.PipelineRunState;
import com.target.kelsaapi.common.vo.s3.S3DbParam;
import com.target.kelsaapi.controllers.PipelineAbstractController;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RestController
@RequestMapping("/marketing_ingest_pipelines/v2")
@Slf4j
@Component("pipelineControllerv2")
public class PipelineController extends PipelineAbstractController {


    @Autowired
    public PipelineController(PipelineRunStateService runStateService,
                              ControllerValidator validator,
                              S3DbParamStateService s3DbParamStateService,
                              PipelineRunnerListener pipelineRunnerListener) {
        super(validator, runStateService, s3DbParamStateService, pipelineRunnerListener);
    }

    @PostMapping("/")
    @ResponseBody
    public PipelineRunStatusResponses requestPipelineRun(@RequestHeader("x-api-key") String key,
                                                         @RequestHeader("Authorization") String auth,
                                                         @RequestHeader("x-tgt-memberof") String memberships,
                                                         @RequestBody PipelineRunRequest pipelineRunRequest)
            throws
            RuntimeException, ParseException, ConfigurationException {

        String targetFilepath;
        ApplicationConstants.Sources source;
        List<String> reportTypes;
        String src;
        Boolean reportCheck=false;

        //Get S3 parameter from database if any else will get from application constant
        S3DbParam s3DbParam= getS3DbParam(pipelineRunRequest.getSource().toUpperCase() + pipelineRunRequest.getReportType().toUpperCase());

        if (s3DbParam.getReportType().isEmpty()) {
            //Validate source
            source = validateSources(pipelineRunRequest.getSource());
            src = pipelineRunRequest.getSource();
            //Validate report type
            reportTypes = validateReportTypes(pipelineRunRequest.getReportType(), source);
        } else {
            log.info("Valid source {} and report type {} validated against PGDB table mdf_s3_parameter entry", pipelineRunRequest.getSource(),pipelineRunRequest.getReportType());
            source = validateSources(ApplicationConstants.Sources.S3.name());
            src = pipelineRunRequest.getSource();
            reportTypes = new ArrayList<String>(Arrays.asList(pipelineRunRequest.getReportType().toUpperCase().split(",")));
        }

        //Validate input date formats & split into a list of formatted date strings in between the start/end dates
        String startDate = pipelineRunRequest.getStartDate();
        String endDate = pipelineRunRequest.getEndDate();
        List<String> datesList = validateDates(startDate, endDate);

        //Validate Memberships
        validateMembers(memberships);

        //Validate File Paths
        String filePath = pipelineRunRequest.getTargetPath();
        validateFilePath(filePath);


        //For running in parallel
        List<PipelineRunStatusResponse> pipelineIDs = new ArrayList<>();

        for (String s : datesList) {

            for (String reportType : reportTypes) {
                if (s3DbParam.getReportType().isEmpty()){
                    reportCheck = validator.identifyS3ReportTypes(source, reportType);
                } else {
                    reportCheck =  validator.identifyS3DbReportTypes(src,reportType);

                }

                if (reportCheck)
                {
                    log.info("It is a S3 type of source and report type:: {} {}", src.toUpperCase(), reportType);
                    targetFilepath = filePath;
                }
                else {
                    log.info("It is a API type of source and report type:: {} {}", src.toUpperCase(), reportType);
                    targetFilepath = CommonUtils.generateFilePath(source, filePath,
                            s, s, reportType);}
                //Initialize the Pipeline run state object
                PipelineRunState runState = new PipelineRunState(
                        PipelineStates.INITIALIZED,
                        src.toLowerCase(),
                        s,
                        s,
                        targetFilepath,
                        reportType
                );
                asyncRequest(runState, pipelineIDs);
            }

        }
        //Return list of PipelineRunStatusResponse
        return new PipelineRunStatusResponses(pipelineIDs);
    }

    @GetMapping("/{pipeline_run_id}")
    @ResponseBody
    public PipelineRunStatusResponse getPipelineStatus(@RequestHeader("x-api-key") String key,
                                                       @RequestHeader("Authorization") String auth,
                                                       @RequestHeader("x-tgt-memberof") String memberships,
                                                       @PathVariable("pipeline_run_id") String pipelineId) {


        return getPipelineState(memberships, pipelineId);
    }

}
