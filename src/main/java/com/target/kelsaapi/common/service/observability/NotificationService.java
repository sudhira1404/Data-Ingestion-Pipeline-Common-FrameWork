package com.target.kelsaapi.common.service.observability;

import com.target.kelsaapi.common.constants.ApplicationConstants;
import org.springframework.util.StopWatch;

/**
 * Interface having details of various notification mechanisms
 * @since 1.0
 */
public interface NotificationService {
    void sendPipelineNotification(ApplicationConstants.slackMessageType messageType, StopWatch stopWatch,
                                  String sourceSystem, String startDate, String endDate, String landingFile,
                                  String batchRequestStatus, String reportType
    );
}
