package com.target.kelsaapi.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.HttpException;
import com.target.kelsaapi.common.vo.HttpCustomResponse;
import com.target.kelsaapi.common.vo.Oauth;
import com.target.kelsaapi.common.vo.snapchat.SnapChatCampaignResponse;
import com.target.kelsaapi.common.vo.snapchat.SnapChatStatsResponse;
import lombok.extern.slf4j.Slf4j;
import org.javatuples.Pair;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.lang.Nullable;
import org.springframework.web.util.UriComponentsBuilder;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class SnapChatUtils {

    public static String buildUrl(String baseUrl, String version, String parentEndpoint, String id, String childEndpoint) {
        return baseUrl + "/" + version + "/" + parentEndpoint + "/" + id + "/" + childEndpoint;
    }

    public static String buildDecoratedUrl(String baseUrl, ApplicationConstants.SnapChatReportTypes reportTypes, @Nullable String startDate, @Nullable String endDate) {
        String url;
        if (reportTypes.equals(ApplicationConstants.SnapChatReportTypes.STATS)) {
            url = decorateUrlWithStatsQueries(baseUrl,startDate,endDate);
            log.debug("SnapChat Campaign Stats url: {}", url);
        } else {
            url = decorateUrlWithCampaignQueries(baseUrl);
            log.debug("SnapChat Campaign Details url: {}", url);
        }
        return url;
    }

    public static Pair<String, String> transformDatesToTimes(String startDate, String endDate, String timeZoneName) {
        return new Pair<>(
                formatDateTime(startDate, "beginning", timeZoneName),
                formatDateTime(endDate, "ending", timeZoneName));
    }

    public static String formatDateTime(String dateString, String beginOrEndTime, String timeZoneName) {
        ZoneId timeZone = ZoneId.of(timeZoneName);
        LocalTime beginningTime = LocalTime.ofSecondOfDay(0);
        LocalDate adjustedDate;
        if (beginOrEndTime.equals("beginning")) {
            adjustedDate = LocalDate.parse(dateString);
        } else {
            adjustedDate = LocalDate.parse(dateString).plusDays(1);
        }
        return DateTimeFormatter.ISO_INSTANT.format(
                ZonedDateTime.of(
                        LocalDateTime.of(
                                adjustedDate,
                                beginningTime
                        ),
                        timeZone
                )
        );
    }

    public static Map<String,String> setHttpHeaders(Oauth oauth) {
        Map<String, String> header =  new LinkedHashMap<>();
        header.put("Authorization",oauth.getOAuthToken());
        header.put("Content-Type", MediaType.APPLICATION_FORM_URLENCODED.toString());
        return header;
    }

    public static String decorateUrlWithStatsQueries(String url, String startTime, String endTime) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(url)
                .queryParam("breakdown", "campaign")
                .queryParam("fields", "impressions,swipes,quartile_1,quartile_2,quartile_3,view_completion," +
                        "attachment_total_view_time_millis,spend,video_views,shares,total_installs,conversion_purchases," +
                        "conversion_purchases_value,conversion_save,conversion_start_checkout,conversion_add_cart," +
                        "conversion_view_content,conversion_add_billing,conversion_sign_ups,conversion_searches," +
                        "conversion_level_completes,conversion_app_opens,conversion_page_views,screen_time_millis")
                .queryParam("granularity", "DAY")
                .queryParam("start_time", startTime)
                .queryParam("end_time", endTime)
                .queryParam("swipe_up_attribution_window", "28_DAY")
                .queryParam("view_attribution_window", "1_DAY")
                .queryParam("report_dimension", "dma")
                .queryParam("limit", "1000");
        return builder.build().toString();
    }

    public static String decorateUrlWithCampaignQueries(String url) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(url)
                .queryParam("limit", "1000");
        return builder.build().toString();
    }

    public static String getAttributePathInMessage(ApplicationConstants.SnapChatReportTypes reportTypes) {
        String attributePathInMessage;
        if (reportTypes.equals(ApplicationConstants.SnapChatReportTypes.STATS)) {
            attributePathInMessage = "/timeseries_stats/0/timeseries_stat/paging/next_link";
        } else {
            attributePathInMessage = "/paging/next_link";
        }
        return attributePathInMessage;
    }



    public static String getNextLink(String deserializeResponse, ObjectMapper mapper,String attributePathInMessage) throws JsonProcessingException {

        JsonNode deserializeResponseMapper = mapper.readTree(deserializeResponse);
        Map<String, JsonNode> map = new JsonFlattener(deserializeResponseMapper).flatten();
        String nextLink="";
        if (  map.get(attributePathInMessage) != null ) {
            JsonNode attributeValue = map.get(attributePathInMessage);
            nextLink = attributeValue.asText();
            if (nextLink != null && !nextLink.trim().isEmpty() && !nextLink.trim().equals("[]")) {
                log.info("nextLink: {}", nextLink);
            }
            else {
                log.info("No pagination next link available.next_link attribute in the response is empty");
            }
        }
        else {
            log.info("No pagination next link available.next_link attribute in the response is empty");
        }

        return (nextLink);

    }

    public static String getHttpResponse(HttpCustomResponse response) throws HttpException {
        String deserializedResponse = null;
        Long statusCode ;
        try {
            statusCode = response.getStatusCode();
        } catch (NullPointerException e) {
            throw new HttpException(e.getMessage(), e.getCause());
        }
        if (statusCode.equals((long) HttpStatus.OK.value())) {
            log.info("Successfully retrieved SnapChat response");
            deserializedResponse = response.getBody();
        } else {
            String errorMessage = "Failed to retrieve SnapChat response details. Status code: " + response.getStatusCode().toString();
            log.error(errorMessage);
            log.error("Response headers: {}",response.getHeaders().toString());
            log.error("Response body: {}",response.getBody());
            throw new HttpException(errorMessage);
        }
        return deserializedResponse;
    }

    public static ArrayList<String> decorateHttpResponse(ArrayList<String> statsHttpResponses, String adAccountId, String adAccountName, ApplicationConstants.SnapChatReportTypes reportType) {
        ArrayList<String> decoratedResponse = new ArrayList<>();
        statsHttpResponses.forEach(response -> {
            if (reportType.equals(ApplicationConstants.SnapChatReportTypes.STATS)) {
                SnapChatStatsResponse serdeResponse = new SnapChatStatsResponse(response, adAccountId, adAccountName);
                decoratedResponse.addAll(serdeResponse.getStats());
            } else {
                SnapChatCampaignResponse serdeResponse = new SnapChatCampaignResponse(response, adAccountId, adAccountName);
                decoratedResponse.addAll(serdeResponse.getCampaigns());
            }
        });
        return decoratedResponse;
    }

}