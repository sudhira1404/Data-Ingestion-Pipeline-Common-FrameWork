package com.target.kelsaapi.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility for sending slack message
 */
@Slf4j
public class SlackNotificationUtil {


    /**
     * Build message to be sent to slack
     *
     * @param environment environment of usage like dev/prod
     * @param icon        icon to be used in the message
     * @param message     text message
     * @param channel     channel name to be posted to
     * @param userName    Name of the user with whom message will be stnt
     * @return Payload message to be sent to slack api
     * @throws JsonProcessingException
     */
    public static String getMessagePayload(String environment, String icon, String message, String channel, String userName) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> slackMessage = new HashMap<>();

        StringBuffer buffer = new StringBuffer();
        buffer.append("```");
        if (environment != null)
            buffer.append("Env :" + environment + "\n");
        if (message == null)
            message = "";
        buffer.append(message + "```");
        slackMessage.put("icon_emoji", icon);
        slackMessage.put(ApplicationConstants.TEXT, buffer.toString());
        slackMessage.put(ApplicationConstants.CHANNEL, channel);

        if (userName != null) {
            slackMessage.put(ApplicationConstants.USERNAME, userName);
        }

        String payload = mapper.writeValueAsString(slackMessage);
        log.info(payload);
        return payload;
    }

    /**
     * Check if slack is enabled
     * @param notification
     * @return status of slack config
     */
    public static Boolean isSlackEnabled(PipelineConfig.Notification notification){
        Boolean validSlackConfig = false;
        if (notification != null && notification.getSlack() != null) {
            validSlackConfig = true;
        }
        log.info("Config status of slackConfig {}", validSlackConfig);
        return validSlackConfig;
    }
}
