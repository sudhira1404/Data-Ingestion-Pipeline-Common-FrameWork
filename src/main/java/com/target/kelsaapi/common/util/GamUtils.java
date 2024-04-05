package com.target.kelsaapi.common.util;

import com.google.api.ads.admanager.axis.utils.v202311.DateTimes;
import com.google.api.ads.admanager.axis.utils.v202311.ReportDownloader;
import com.google.api.ads.admanager.axis.utils.v202311.StatementBuilder;
import com.google.api.ads.admanager.axis.v202311.*;
import com.google.api.client.util.BackOff;
import com.google.common.collect.Lists;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.GamException;
import com.target.kelsaapi.common.vo.google.response.admanager.delivery.GamOrder;
import com.target.kelsaapi.common.vo.google.response.admanager.delivery.GamOrderResponse;
import com.target.kelsaapi.common.vo.google.response.admanager.forecast.GamForecastResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.data.util.Pair;
import org.springframework.lang.Nullable;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.RemoteException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Static utility methods supporting Gam services
 */
@Slf4j
public class GamUtils {

    /**
     * Used to initialize a {@link Statement} that will fetch {@link Order} data with.
     *
     * @param startDate The start date in yyyy-MM-dd format.
     * @param endDate The end date in yyyy-MM-dd format.
     * @return A {@link Statement} to retrieve {@link Order} objects with
     */
    public static Statement generateOrderStatement(String startDate, String endDate) {
        return initStatementBuilder()
                .where("isArchived = :isArch AND startDateTime <= :start_date AND endDateTime >= :end_date")
                .withBindVariableValue("isArch", false)
                .withBindVariableValue("start_date", startDate)
                .withBindVariableValue("end_date", endDate)
                .toStatement();
    }

    /**
     * Used to initialize a {@link StatementBuilder} that will fetch all {@link LineItem} objects which are filtered
     * using a comma-separated string of {@link Order#getId} ids.
     *
     * @param orderIds The comma-separated string of {@link Order#getId} ids to retrieve {@link LineItem} records from.
     * @return A {@link StatementBuilder} to retrieve {@link LineItem} objects with
     */
    public static StatementBuilder generateLineItemStatement(String orderIds) {
        return initStatementBuilder()
                .where("orderId IN (" + orderIds + ") AND isArchived = :isArch AND status != :status")
                .withBindVariableValue("isArch", false)
                .withBindVariableValue("status", "DRAFT");
    }

    /**
     * Used to initialize a {@link Statement} that will fetch all geographical locations matching the specified type.
     *
     * @param type The type of geographical locations to retrieve.
     * @return A {@link Statement} to retrieve geographical locations with.
     */
    public static Statement generateGeoStatement(String type) {
        return initStatementBuilder()
                .select("Name")
                .from("Geo_Target")
                .where("Type = :type")
                .withBindVariableValue("type", type)
                .toStatement();
    }

    /**
     * Initializes a new instance of a {@link StatementBuilder}
     *
     * @return A new instance of a {@link StatementBuilder}
     */
    private static StatementBuilder initStatementBuilder() {
        return new StatementBuilder();
    }

    /**
     * Used to download report results in csv format
     *
     * @param reportDownloader The initialized {@link ReportDownloader} containing the remote location which to download
     *                         from.
     * @return A list of the comma-separated results obtained from the downloaded report
     * @throws MalformedURLException, ApiException, RemoteException Indicates a failure has occurred while downloading the results
     */
    public static URL downloadReportToURL(ReportDownloader reportDownloader) throws MalformedURLException, RemoteException {
        ReportDownloadOptions options = new ReportDownloadOptions();
        options.setExportFormat(ExportFormat.CSV_DUMP); //default is CSV_DUMP; other options are TSV, TSV_EXCEL, XML, XLSX
        options.setUseGzipCompression(false); //default is true
        options.setIncludeReportProperties(false); //default is false
        options.setIncludeTotalsRow(false); //default is true for all formats except CSV_DUMP
        return reportDownloader.getDownloadUrl(options);
    }

    /**
     * Used to download report results in csv format
     *
     * @param reportDownloader The initialized {@link ReportDownloader} containing the remote location which to download
     *                         from.
     * @return A list of the comma-separated results obtained from the downloaded report
     * @throws IOException Indicates a failure has occurred while downloading the results
     */
    public static List<String> downloadReportToList(ReportDownloader reportDownloader) throws IOException {
        ReportDownloadOptions options = new ReportDownloadOptions();
        options.setExportFormat(ExportFormat.CSV_DUMP); //default is CSV_DUMP; other options are TSV, TSV_EXCEL, XML, XLSX
        options.setUseGzipCompression(false); //default is true
        options.setIncludeReportProperties(false); //default is false
        options.setIncludeTotalsRow(false); //default is true for all formats except CSV_DUMP
        return reportDownloader.getReportAsCharSource(options).readLines();
    }

    /**
     * Converts a {@link DateTime} to an ISO formatted string in UTC timezone
     *
     * @param dt The {@link DateTime} which will be converted to a string
     * @return An ISO formatted string in UTC timezone
     */
    public static String dateTimeToString(@Nullable DateTime dt) {
        if (dt != null) {
            return DateTimes.toStringForTimeZone(dt, "UTC-00:00") + "Z";
        } else {
            return null;
        }
    }

    public static LocalDate dateTimeToLocalDate(DateTime dt) {
        String timestampPattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        String stringDateTime = dateTimeToString(dt);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timestampPattern);
        LocalDateTime localDateTime = LocalDateTime.from(formatter.parse(stringDateTime));
        return LocalDate.from(localDateTime);
    }

    public static String instantToCentralZoneString(Instant dt) {
        String timestampPattern = "yyyy-MM-dd'T'HH:mm:ss";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timestampPattern)
                .withZone(ZoneId.of("America/Chicago"));
        return formatter.format(dt);
    }

    public static Boolean reportDateIsToday(Instant today, String reportDate) {
        LocalDate todayDate = LocalDate.ofInstant(today, ZoneId.of("America/Chicago"));
        LocalDate nowDate = LocalDate.parse(reportDate);
        return todayDate.equals(nowDate);
    }

    /**
     * Creates a string containing all the corresponding order ids from a {@link GamOrderResponse#getOrders()} separated
     * by commas.
     *
     * @param orderResponse The response object holding order objects.
     * @return String of comma-separated order ids from the response object.
     */
    public static String orderIdsToCommaString(GamOrderResponse orderResponse) {
        List<String> orderIds = Lists.newArrayList();
        for (GamOrder order : orderResponse.getOrders()) {
            orderIds.add(order.getId().toString());
        }
        return StringUtils.join(orderIds, ',');
    }

    public static Pair<Integer,Long> wait(int attempt, long retryInterval, BackOff backOff) throws GamException, IOException, InterruptedException {
        if (backOff.nextBackOffMillis() == BackOff.STOP) {
            throw new GamException("Operation did not succeed before max wait time exceeded!");
        } else {
            Thread.sleep(retryInterval);
            attempt++;
            retryInterval=backOff.nextBackOffMillis();
        }
        return Pair.of(attempt,retryInterval);
    }

    public static List<Long> pickRandomProspectiveLineItems(List<Long> list, int n, Random r) {
        int length = list.size();

        if (length < n) return list;

        for (int i = length - 1; i>=length - n; --i) {
            Collections.swap(list, i, r.nextInt(i + 1));
        }
        return Lists.newArrayList(list.subList(length - n, length));
    }

    public static Triple<Boolean, Integer, Long> waitOrRetryForecastRequest(@Nullable GamForecastResponse response,
                                                                            Integer attempt,
                                                                            Long retryInterval,
                                                                            BackOff backOff,
                                                                            ApplicationConstants.GamForecastTypes forecastType)
            throws IOException, GamException, InterruptedException {
        boolean retry;
        Integer newAttempt = attempt;
        Long newInterval = retryInterval;
        if (response == null) {
            log.warn("Attempt {} to generate {} forecast failed!", attempt, forecastType.name().toLowerCase());
            if (backOff.nextBackOffMillis() != BackOff.STOP) {
                log.warn("Next attempt will be made after sleeping {} milliseconds", retryInterval);

                Pair<Integer, Long> wait = GamUtils.wait(attempt, retryInterval, backOff);
                newAttempt = wait.getFirst();
                newInterval = wait.getSecond();
                retry = true;
            } else {
                log.error("Max wait time has been reached after "+ attempt +" attempts, and no more attempts will be made!!");
                retry = false;
            }
        } else {
            if (response.getFailureReason() == null) {
                log.info("Successfully generated {} forecast on attempt {}.", forecastType.name().toLowerCase(), attempt);
            } else {
                log.error("Failed to generate {} forecast on attempt {} due to {}, and will not be retried.",
                        forecastType.name().toLowerCase(), attempt, response.getFailureReason());
            }
            retry = false;
        }
        return Triple.of(retry, newAttempt, newInterval);
    }

    public static List<Long> getContendingLineItems(List<Long> allContendingIdsForOriginalLineItem, Long originalLineItem, int contendingLineItemsSize) {
        int initialNumber = allContendingIdsForOriginalLineItem.size();

        if (initialNumber <= contendingLineItemsSize && initialNumber > 0) {
            log.debug("There are {} contending line items for Line Item ID : {}. " +
                            "This is under the threshhold set in configs of {} contending line items. " +
                            "Will request Delivery Forecast for all contending line items.",
                    initialNumber, originalLineItem, contendingLineItemsSize);
            return allContendingIdsForOriginalLineItem;
        } else {
            log.debug("There are more than {} contending line items for Line Item ID {}. " +
                            "Narrowing the list down to top {} contending line items by contending impressions counts.",
                    contendingLineItemsSize, originalLineItem, contendingLineItemsSize);

            Long[] idWithContending = new Long[contendingLineItemsSize + 1];
            idWithContending[0] = originalLineItem;

            for (int counter = 1; counter < idWithContending.length; counter++) {
                idWithContending[counter] = allContendingIdsForOriginalLineItem.get(counter - 1);
            }

            return Arrays.stream(idWithContending).distinct().toList();
        }

    }
}
