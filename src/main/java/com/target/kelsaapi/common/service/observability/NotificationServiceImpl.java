package com.target.kelsaapi.common.service.observability;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.common.exceptions.HttpException;
import com.target.kelsaapi.common.service.rest.HttpService;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.util.SlackNotificationUtil;
import com.target.kelsaapi.common.vo.HttpCustomResponse;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import java.util.Map;

@Slf4j
@Service("notificationService")
public class NotificationServiceImpl implements NotificationService {

    private final PipelineConfig config;

    private final HttpService httpService;

    @Autowired
    NotificationServiceImpl(PipelineConfig config, HttpService httpService) {
        this.config = config;
        this.httpService = httpService;
    }

    @Override
    public void sendPipelineNotification(ApplicationConstants.slackMessageType messageType, StopWatch stopWatch,
                                         String sourceSystem, String startDate, String endDate, String landingFile,
                                         String batchRequestStatus, String reportType
                                         )  {
        String message;
        PipelineConfig.Slack slack = config.apiconfig.notification.slack;

        message = config.apiconfig.appName +
                    "\nRequested source: " + sourceSystem +
                    "\nRequested report type: " + reportType +
                    "\nRequested start date: " + startDate +
                    "\nRequested end date: " + endDate +
                    "\nRequested landing file: " + landingFile +
                    "\nPipeline status: " + batchRequestStatus +
                    "\nTime taken for execution\n" +
                    CommonUtils.prettyPrintStopWatchSeconds(stopWatch);

        try {
            sendSlackMessage(slack, message, messageType);
        } catch (JsonProcessingException | ConfigurationException e) {
            log.error("Exception thrown while trying to send Slack message", e);
        }
    }

    /**
     * Send message to slack
     *
     * @param webhook webhook url
     * @param payload slack payload
     * @return status of message push
     */
    private Boolean sendMessage(String webhook, String payload) {
        try {
            Map<String,String> headers = Map.of(ApplicationConstants.CONTENTTYPE, ApplicationConstants.JSONDATA);
            HttpCustomResponse response = httpService.post(webhook, headers, payload);
            if (response.getStatusCode().equals((long) HttpStatus.OK.value())) {
                log.info("success in sending message {}", response.getBody());
                return true;
            } else {
                throw new HttpException("Failure in sending message");
            }
        } catch (Exception e) {
            log.error("Failure in sending message", e);
            return false;
        }
    }

    private void sendSlackMessage(PipelineConfig.Slack slack, String message, ApplicationConstants.slackMessageType eventType) throws JsonProcessingException, ConfigurationException {

        String icon;
        PipelineConfig.SlackMessage slackMessage;

        switch (eventType) {
            case SUCCESS -> {
                slackMessage = slack.getSuccess();
                icon = ":smile:";
            }
            case FAILURE -> {
                slackMessage = slack.getFailure();
                icon = ":x:";
            }
            default -> throw new ConfigurationException("Wrong message type");
        }
        String payload = SlackNotificationUtil.getMessagePayload(slackMessage.getEnvironment(), icon, message, slackMessage.getChannel(), slackMessage.getUsername());
        Boolean success = sendMessage(slackMessage.getWebhookurl(), payload);
        log.info("Status of slack message {} ", success);
    }
}
