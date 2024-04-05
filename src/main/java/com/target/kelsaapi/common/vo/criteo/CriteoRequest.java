package com.target.kelsaapi.common.vo.criteo;

import com.target.kelsaapi.common.vo.pinterest.PinterestReportRequest;
import lombok.Getter;
import org.apache.commons.compress.utils.Lists;

import java.util.HashMap;
import java.util.List;

public class CriteoRequest {
    @Getter
    private final String end_date;
    @Getter
    private final String start_date;
    @Getter
    private final String report_type;

    @Getter
    private final String directory;

    @Getter
    private final String fileName;


    public CriteoRequest(String startDate, String endDate, String reportType) {
        this.start_date = startDate;
        this.end_date = endDate;
        this.report_type = reportType;
        this.directory = getDirectory(getReport_type());
        this.fileName = getFileName(reportType,startDate);

    }

    private String getDirectory(String report_type){
        HashMap<String, String> filePaths = new HashMap<String, String>();

        filePaths.put("criteo_campaign_dly_snapshot", "");
        filePaths.put("criteo_keyword_dly_snapshot", "");

        return  filePaths.get(report_type);
    }

    private String getFileName(String reportType, String startDate){
        String fileName = reportType + "_" + startDate + ".tsv";
        return fileName;
    }
}
