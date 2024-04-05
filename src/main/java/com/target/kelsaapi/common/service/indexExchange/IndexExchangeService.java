package com.target.kelsaapi.common.service.indexExchange;

import com.target.kelsaapi.common.exceptions.HttpException;
import com.target.kelsaapi.common.exceptions.HttpRetryableException;
import com.target.kelsaapi.common.vo.Oauth;
import com.target.kelsaapi.common.vo.indexexchange.ReportsList;

import java.util.ArrayList;
import java.util.Map;

public interface IndexExchangeService {

    Map<String, String> setHeaderMap(Oauth oauth);

    ArrayList<ReportsList> listReports(Map<String, String> headersMap, int reportId)
            throws HttpException, HttpRetryableException;

    String downloadReport(Map<String, String> headersMap, int reportId)
            throws HttpException, HttpRetryableException ;

}
