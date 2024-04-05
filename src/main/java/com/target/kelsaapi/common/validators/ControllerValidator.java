package com.target.kelsaapi.common.validators;

import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.vo.s3.S3DbParam;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

@Slf4j
@Service
public class ControllerValidator {

    @Autowired
    private PipelineConfig pipelineConfig;

    public Boolean validateSources(String source) {
        return Arrays.stream(ApplicationConstants.Sources.values()).anyMatch(value ->
                String.valueOf(value).equals(source.toUpperCase()));
    }

    public Boolean validateReportTypes(ApplicationConstants.Sources source, String reportTypeName) throws ConfigurationException {
        Stream<?> valueStream = CommonUtils.streamReportTypes(source);
        return valueStream.anyMatch(value ->
                (String.valueOf(value).toLowerCase(Locale.ROOT).equals(reportTypeName.toLowerCase(Locale.ROOT))
                        ||
                        (String.valueOf(value).toLowerCase(Locale.ROOT).equals(source.toString().toLowerCase(Locale.ROOT) + reportTypeName.toLowerCase(Locale.ROOT))
                        )
                ));
    }
    public Boolean identifyS3ReportTypes(ApplicationConstants.Sources source, String reportTypeName) throws ConfigurationException {
        Stream<?> valueStream = CommonUtils.streamReportTypes(source);
        return valueStream.anyMatch(value ->
                (String.valueOf(value).toLowerCase(Locale.ROOT).equals(source.toString().toLowerCase(Locale.ROOT) + reportTypeName.toLowerCase(Locale.ROOT))
                )
        );
    }

    public Boolean identifyS3DbReportTypes(String source, String reportTypeName) {

        if ((source.toUpperCase() + reportTypeName.toUpperCase()).equals(S3DbParam.getReportType().toUpperCase())) {
            return true;
        }
        else
        {
            return  false;
        }
    }

    public Boolean validateDateFormats(String start_date, String end_date) {
        List<String> dateList = List.of(start_date, end_date);
        int bothValid = 0;
        for (String date : dateList) {
            try {
                CommonUtils.validateDateFormat(date);
                bothValid++;
            } catch (ParseException e) {
                log.error("Given date is incorrectly formatted: " + date);
            }
        }
        return bothValid == 2;
    }

    public Boolean validateMembership(String memberships) {
        String authorizedGroup = "CN=" + pipelineConfig.getApiconfig().getAuthorizedGroup();
        return memberships.contains(authorizedGroup);
    }

    /**
     * @param targetPath
     * @return boolean
     */
    public Boolean validatePath(String targetPath) {
        char first = 0;
        char last = 0;

        if (targetPath.isEmpty()) {
            log.error("Not a valid targetPath. Please provide a valid path that starts with / and ends with /");
        } else {
            // First character of a string
             first = targetPath.charAt(0);
            // Last character of a string
             last = targetPath.charAt(targetPath.length() - 1);
        }
        return first == '/' && last == '/';
    }
}
