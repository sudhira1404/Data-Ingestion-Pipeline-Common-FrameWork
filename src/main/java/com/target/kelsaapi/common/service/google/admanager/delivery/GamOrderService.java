package com.target.kelsaapi.common.service.google.admanager.delivery;

import com.google.api.ads.admanager.axis.v202311.Order;
import com.google.api.ads.admanager.axis.v202311.OrderPage;
import com.google.api.ads.admanager.axis.v202311.OrderServiceInterface;
import com.google.api.ads.admanager.axis.v202311.Statement;
import com.google.api.ads.admanager.lib.client.AdManagerSession;
import com.google.api.client.auth.oauth2.Credential;
import com.google.errorprone.annotations.DoNotCall;
import com.target.kelsaapi.common.exceptions.GamException;
import com.target.kelsaapi.common.service.file.LocalFileWriterService;
import com.target.kelsaapi.common.service.google.admanager.AdManagerSessionServicesFactoryInterface;
import com.target.kelsaapi.common.service.google.admanager.GamServiceInterface;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.vo.google.request.admanager.GamRequest;
import com.target.kelsaapi.common.vo.google.request.admanager.delivery.GamOrderRequest;
import com.target.kelsaapi.common.vo.google.response.admanager.GamResponse;
import com.target.kelsaapi.common.vo.google.response.admanager.delivery.GamOrderResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;

/**
 * A service layer that interacts with the Google Ad Manager SDK to retrieve Order Data
 */
@Service
@Slf4j
public class GamOrderService implements GamServiceInterface {

    private final AdManagerSessionServicesFactoryInterface adManagerServices;

    private final LocalFileWriterService localFileWriterService;

    /**
     * The constructor used by Spring Framework to spin up and autowire this singleton service
     *
     * @param adManagerServices The {@link AdManagerSessionServicesFactoryInterface} for initializing Ad Manager Services from
     * @param localFileWriterService The {@link LocalFileWriterService} for initializing the local file writing service from
     */
    @Autowired
    public GamOrderService(AdManagerSessionServicesFactoryInterface adManagerServices,
                           LocalFileWriterService localFileWriterService) {
        this.adManagerServices = adManagerServices;
        this.localFileWriterService = localFileWriterService;
    }

    /**
     * Initializes a new {@link AdManagerSession} to use the {@link OrderServiceInterface}
     * and the provided {@link Statement} to retrieve an array of {@link Order} objects which then are passed back
     * to the caller in a new {@link GamOrderResponse}
     *
     * @param statement The {@link com.google.api.ads.admanager.axis.utils.v202311.Pql} formatted {@link Statement} for
     *                  retrieving an {@link OrderPage} containing all {@link Order} objects matching the filter conditions.
     * @param credential A refreshed Google {@link Credential} object.
     * @param startDate The start date in yyyy-MM-dd format.
     * @param endDate The end date in yyyy-MM-dd format.
     * @param tempFiles A list of {@link Path}. This list will be added to upon successfully writing the orders locally and
     *                  passed back in the corresponding {@link GamOrderResponse} object.
     * @return A new {@link GamOrderResponse} object.
     * @throws GamException A wrapper exception class containing the message and cause from the underlying exception thrown.
     */
    private GamOrderResponse getOrders(Statement statement, Credential credential, String startDate, String endDate,
                                       List<Path> tempFiles, String pipelineRunId) throws GamException, IOException {
        String orderFile = CommonUtils.generateTempFileRootPath() + pipelineRunId + "_order_report-" + startDate + ".json";
        try {
            Instant now = Instant.now();
            log.info("Initialize new Ad Manager Session at {}", now);
            AdManagerSession newSession = adManagerServices.initAdManagerSession(credential);

            log.info("Initialize new Order Service Interface");
            OrderServiceInterface orderService = adManagerServices.initOrderService(newSession);

            log.info("Initialize request for orders...");
            OrderPage orderPage = orderService.getOrdersByStatement(statement);

            log.info("Download orders...");
            Order[] orders = orderPage.getResults();

            log.info("Order retrieval completed successfully!");
            GamOrderResponse gor = new GamOrderResponse(orders, startDate, now);

            Boolean writeOrderFile = writeResponses(gor, orderFile);
            if (Boolean.TRUE.equals(writeOrderFile)) {
                tempFiles.add(Paths.get(orderFile));
                gor.setTempFileList(tempFiles);
                log.info("Successfully wrote Gam Order data to local disk.");
            } else {
                throw new IOException("Unable to write Gam Order file to local disk");
            }
            return gor;

        } catch (Exception e) {
            log.error(e.getMessage(), e.getCause());
            throw new GamException(e.getMessage(), e.getCause());
        }
    }

    /**
     * Uses the {@link GamOrderResponse#getJsonOrders()} method and lands them to the specified local file.
     *
     * @param response The {@link GamOrderResponse} object.
     * @param orderFileName Name of the local file to land results to.
     * @return True if successful writing to local file. False if it could not write.
     */
    private Boolean writeResponses(GamOrderResponse response, String orderFileName) {
        List<String> responseList = response.getJsonOrders();
        log.info("Successfully retrieved {} orders. Attempting to write to local file now...", responseList.size());
        return localFileWriterService.writeLocalFile(responseList, orderFileName, false);
    }

    @Override
    public GamResponse get(GamRequest request, Credential credential) throws GamException {
        GamOrderRequest orderRequest = (GamOrderRequest) request;
        try {
            return getOrders(orderRequest.getStatement(), credential, orderRequest.getStartDate(), orderRequest.getEndDate(),
                    orderRequest.getTempFileList(), orderRequest.getPipelineRunId());
        } catch (IOException e) {
            throw new GamException(e.getMessage(),e.getCause());
        }
    }

    @Deprecated
    @DoNotCall
    @Override
    public GamResponse get(Credential credential) throws GamException {
        return null;
    }

    @Deprecated
    @DoNotCall
    @Override
    public Credential get() throws GamException {
        return null;
    }
}
