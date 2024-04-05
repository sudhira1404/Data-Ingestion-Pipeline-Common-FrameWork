package com.target.kelsaapi.pipelines.config;

import com.target.kelsaapi.common.service.config.ConfigValidatorImpl;
import com.target.kelsaapi.common.vo.xandr.Report;
import com.target.platform.connector.config.ConfigSource;
import com.target.platform.connector.config.FileSource;
import com.target.platform.connector.func.StringUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.configuration.ConfigurationConverter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;

/**
 * Configure pipeline properties and validate them
 */
@Configuration
@Data
@Slf4j
@ConfigurationProperties
public class PipelineConfig implements InitializingBean {
    public Apiconfig apiconfig;

    @Autowired
    ConfigValidatorImpl configValidator;


    @Override
    public void afterPropertiesSet() throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        //Set the passswords from ConfigSource

        try {
            log.info("Reading values from TAP");
            //Reading secrets
            if (apiconfig.getSecrets() != null) {
                for (Secrets secret : apiconfig.getSecrets()) {
                    String fieldName = secret.getFieldname();
                    log.info("Looking for the field {}", fieldName);

                    if (PropertyUtils.getNestedProperty(this.apiconfig, fieldName) == null  || StringUtils.isEmpty((CharSequence) PropertyUtils.getNestedProperty(this.apiconfig, fieldName))) {
                        log.info("Filling secret for " + fieldName);
                        FileSource fieldSource = ConfigSource.file(secret.getTapkey());
                        if(fieldSource != null){
                            PropertyUtils.setNestedProperty(this.apiconfig, fieldName, fieldSource.getString());
                        }else {
                            log.error("Secret is not configured for {}",secret.getTapkey());
                        }
                    }else {
                        log.info("Considered the value configured in application.yml for the property {}",fieldName);
                    }

                }
            }
            configValidator.validateConfig(this);

        } catch (InvocationTargetException e) {
            log.error("Error in initializing beans with secrets", e);
            throw new InvocationTargetException(e, "Field name wrong or empty");
        } catch (IllegalAccessException | NoSuchMethodException e) {
            log.error("Error in initializing beans with secrets", e);
            throw e;

        } catch (Exception e) {
            log.error("Error in processing",e);
        }

    }

    @Data
    public static class Apiconfig {
        public String appName;
        public String blossomid;
        public String teamname;
        public String email;
        public String description;
        public String authorizedGroup;
        public Source source;
        public Hdfsdetails hdfsdetails;
        public Notification notification;
        public PipelineRunnerListenerThreadPool pipelineRunnerListener;
        public List<Secrets> secrets;
    }

    @Data
    public static class PipelineRunnerListenerThreadPool {
        public ThreadPool threadPool;
    }

    @Data
    public static class Notification {
        public Slack slack;
    }


    @Data
    public static class Slack {
        public SlackMessage success;
        public SlackMessage failure;

    }

    @Data
    public static class SlackMessage {
        public String environment;
        public String channel;
        public String username;
        public String webhookurl;
    }

    @Data
    public static class Source {
        public Boolean cleanupTempFile;
        public Facebook facebook;
        public Tradedesk tradedesk;
        public Pinterest pinterest;
        public SnapChat snapchat;
        public Google google;
        public Salesforce salesforce;
        public Criteo criteo;
        public Xandr xandr;
        public IndexExchange indexExchange;
        public S3SwitchBoard s3SwitchBoard;


    }

    @Data
    public static class Google {
        public MarketingPlatform marketingPlatform;
        public AdManager adManager;

        @Data
        public static class AdManager {
            public String clientId;
            public String applicationName;
            public String networkCode;
            public String jsonKeyFilePath;
            public Delivery delivery;
            public Forecast forecast;

            public org.apache.commons.configuration.Configuration getGamConfig() {
                Properties props = new Properties();
                props.setProperty("api.admanager.applicationName",this.applicationName);
                props.setProperty("api.admanager.networkCode",this.networkCode);
                props.setProperty("api.admanager.jsonKeyFilePath",this.jsonKeyFilePath);
                return ConfigurationConverter.getConfiguration(props);
            }

            @Data
            public static class QueueingAsyncThreads {
                public RetryBackoff retryBackoff;
            }

            @Data
            public static class AsyncThreads {
                public RetryBackoff retryBackoff;
                public int requestTimeoutSeconds;
            }

            @Data
            public static class RetryBackoff {
                public int initialIntervalSeconds;
                public int maxRetryIntervalSeconds;
                public int totalTimeToWaitMinutes;
            }

            @Data
            public static class Sample {
                public Boolean enabled;
                public int size;
            }

            @Data
            public static class Delivery {
                public Sample sample;
                public int paginationSize;
            }

            @Data
            public static class Forecast {
                public int contendingLineItemSize;
                public Sample sample;
                public PipelineConfig.ThreadPool threadPool;
                public QueueingAsyncThreads queueingAsyncThreads;
                public AsyncThreads asyncThreads;
            }

        }

        @Data
        public static class MarketingPlatform {
            public String userName;
            public String jsonKeyFilePath;
        }

    }

    @Data
    public static class Pinterest {
        public String baseUrl;
        public String version;
        public String baseEndpoint;
        public String endpointSuffix;
        public com.target.kelsaapi.pipelines.config.authentication.Oauth2 authentication;
    }

    @Data
    public static class SnapChat {
        public String baseUrl;
        public String version;
        public String accountId;
        public com.target.kelsaapi.pipelines.config.authentication.Oauth2 authentication;
    }


    @Data
    public static class Tradedesk {
        public String baseUrl;
        public String reportsEndPoint;
        public String contractorEndPoint;
        public String advertiserEndPoint;
        public String adgroupAdvertiserEndPoint;
        public String partnerId;
        public com.target.kelsaapi.pipelines.config.authentication.Oauth2 authentication;
    }

    @Data
    public static class Facebook {
        public FacebookContext context;
        public String appId;
        public int sleepIntervalMs;
        public int maxSleepIntervals;
    }

    @Data
    public static class FacebookContext {
        public String accessToken;
        public String appSecret;
        public Boolean debug;
    }

    @Data
    public static class S3SwitchBoard {
        public String bucketName;
        public String regionName;
        public String profileName;
        public String credentialsFileLocation;
        public Integer maxLinesBeforeFlush;
        public Long maxSizeBeforeNewFile;
        public String fileDownloadStatus;
    }

    @Data
    public static class Salesforce {
        public String baseUrl;
        public String clientID;
        public String clientSecret;
        public String userName;
        public String password;
        public com.target.kelsaapi.pipelines.config.authentication.Oauth2 authentication;
    }

    @Data
    public static class Criteo {
        public String hostName;
        public String userName;
        public String password;
        public String privateKeyFile;
    }


    @Data
    public static class Hdfsdetails {
        public String endPoint;
        public String format;
        public String userName;
        public String password;
    }

    @Data
    public static class Secrets {
        public String fieldname;
        public String tapkey;
    }


    @Data
    public static class ThreadPool {
        public int corePoolSize;
        public int maxPoolSize;
        public int queueSize;
    }

    @Data
    public static class Xandr {

        public String baseUrl;
        public String reportEndPoint;
        public String reportStatusEndPoint;
        public String reportDownloadEndPoint;
        public Report report;
        public com.target.kelsaapi.pipelines.config.authentication.Oauth2 authentication;
    }
    @Data
    public static class IndexExchange {

        public String baseUrl;
        public String version;
        public String reportEndPoint;
        public String reportListEndPoint;
        public String reportDownloadEndPoint;
        public String accountId;

        public com.target.kelsaapi.pipelines.config.authentication.Oauth2 authentication;
    }
}
