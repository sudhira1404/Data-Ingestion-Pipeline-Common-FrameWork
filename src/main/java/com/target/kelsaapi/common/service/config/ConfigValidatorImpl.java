package com.target.kelsaapi.common.service.config;

import com.amazonaws.util.StringUtils;
import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import com.target.kelsaapi.pipelines.config.authentication.Oauth2;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static com.target.kelsaapi.common.constants.ApplicationConstants.*;

/**
 * For validating configuration
 *
 * @Author: Sonali.Polaki
 * @since 1.0
 */
@Slf4j
@Service("configValidator")
public class ConfigValidatorImpl implements ConfigValidator {
    /**
     * For Validating the YAML configuration
     *
     * @param pipelineConfig config from yml file
     * @return true/false
     * @throws ConfigurationException
     */
    @Override
    public Boolean validateConfig(PipelineConfig pipelineConfig) throws ConfigurationException {

        Boolean validConfig = true;
        try {
            log.info("Check Sources");
            validConfig = validateSourceProcess(pipelineConfig.getApiconfig().getSource());
            log.info("Check hdfs details");
            validConfig = validateHdfsDetails(pipelineConfig.getApiconfig().getHdfsdetails());
            log.info("Check configured secrets");
            validConfig = validateSecrets(pipelineConfig.getApiconfig().getSecrets());
            log.info("Check PipelineRunnerListener Thread pool configs");
            validConfig = validatePipelineRunnerListener(pipelineConfig.getApiconfig().getPipelineRunnerListener());
        } catch (Exception e) {
            log.error("Error in validating config");
            validConfig = false;
            throw new ConfigurationException("Error in validating config", e);
        }
        return validConfig;

    }

    private Boolean validatePipelineRunnerListener(PipelineConfig.PipelineRunnerListenerThreadPool pipelineRunnerListener) {
        PipelineConfig.ThreadPool threadPoolConfigs = pipelineRunnerListener.threadPool;
        return !CommonUtils.checkIfEmpty("corePoolSize", StringUtils.fromInteger(threadPoolConfigs.getCorePoolSize())) &&
                !CommonUtils.checkIfEmpty("maxPoolSize", StringUtils.fromInteger(threadPoolConfigs.getMaxPoolSize()));

    }

    /**
     * Validating the source details
     *
     * @param source
     * @return
     * @throws ConfigurationException
     */
    public Boolean validateSourceProcess(PipelineConfig.Source source) throws ConfigurationException {
        Boolean allSuccessful = false;

        allSuccessful = validateFacebook(source.getFacebook());
        allSuccessful = validateGam(source.getGoogle().getAdManager());
        allSuccessful = validateTradedesk(source.getTradedesk());
        allSuccessful = validatePinterest(source.getPinterest());
        allSuccessful = validateSnapchat(source.getSnapchat());
        allSuccessful = validateCampaignManager360(source.getGoogle().getMarketingPlatform());
        allSuccessful = validateS3SwitchBoard(source.getS3SwitchBoard());
        return allSuccessful = false;
    }

    private Boolean validateGam(PipelineConfig.Google.AdManager gam) {
        Boolean allValid = true;
        try {
            if (gam.applicationName.isEmpty()) {
                log.error("Required property for GAM source missing: apiconfig.source.google.adManager.applicationName");
                allValid = false;
            }
            if (gam.networkCode.isEmpty()) {
                log.error("Required property for GAM source missing: apiconfig.source.google.adManager.networkCode");
                allValid = false;
            }
            if (gam.jsonKeyFilePath.isEmpty()) {
                log.error("Required property for GAM source missing: apiconfig.source.google.adManager.jsonKeyFilePath");
                allValid = false;
            }
            if (gam.delivery == null) {
                log.error("Required property for GAM Delivery missing: apiconfig.source.google.adManager.delivery");
                allValid = false;
            } else {
                if (gam.delivery.sample == null) {
                    log.error("Required property for GAM Delivery rate control missing: apiconfig.source.google.adManager.delivery.sample");
                    allValid = false;
                } else {
                    if (gam.delivery.sample.enabled == null) {
                        log.error("Required property for GAM Delivery rate control missing: apiconfig.source.google.adManager.delivery.sample.enabled");
                        allValid = false;
                    }
                    if (gam.delivery.sample.size == 0) {
                        log.error("Required property for GAM Delivery rate control missing: apiconfig.source.google.adManager.delivery.sample.size");
                        allValid = false;
                    }
                }
                if (gam.delivery.paginationSize == 0) {
                    log.error("Required property for GAM Delivery rate control missing: apiconfig.source.google.adManager.delivery.paginationSize");
                }
            }

            if (gam.forecast == null) {
                log.error("Required property for GAM Forecasting rate control missing: apiconfig.source.google.adManager.forecast");
                allValid = false;
            } else {
                if (gam.forecast.contendingLineItemSize <= 0) {
                    log.error("Required property for GAM Forecasting contending line item size missing: apiconfig.source.google.adManager.forecast.contendingLineItemSize");
                    allValid = false;
                }
                if (gam.forecast.sample == null) {
                    log.error("Required property for GAM Forecasting rate control missing: apiconfig.source.google.adManager.forecast.sample");
                    allValid = false;
                } else {
                    if (gam.forecast.sample.enabled == null) {
                        log.error("Required property for GAM Forecasting rate control missing: apiconfig.source.google.adManager.forecast.sample.enabled");
                        allValid = false;
                    }
                    if (gam.forecast.sample.size == 0) {
                        log.error("Required property for GAM Forecasting rate control missing: apiconfig.source.google.adManager.forecast.sample.size");
                        allValid = false;
                    }
                }

                if (gam.forecast.threadPool == null) {
                    log.error("Required property for GAM Forecasting parallelism control missing: apiconfig.source.google.adManager.forecast.threadPool");
                    allValid = false;
                } else {
                    if (gam.forecast.threadPool.corePoolSize <= 0) {
                        log.error("Required property for GAM Forecasting parallelism control missing: apiconfig.source.google.adManager.forecast.threadPool.corePoolSize");
                        allValid = false;
                    }
                    if (gam.forecast.threadPool.maxPoolSize <= 0) {
                        log.error("Required property for GAM Forecasting parallelism control missing: apiconfig.source.google.adManager.forecast.threadPool.maxPoolSize");
                        allValid = false;
                    }
                    if (gam.forecast.threadPool.queueSize <= 0) {
                        log.error("Required property for GAM Forecasting parallelism control missing: apiconfig.source.google.adManager.forecast.threadPool.queueSize");
                        allValid = false;
                    }
                }


                if (gam.forecast.queueingAsyncThreads == null) {
                    log.error("Required property for GAM Forecasting async queueing control missing: apiconfig.source.google.adManager.forecast.queueingAsyncThreads");
                    allValid = false;
                } else {
                    if (gam.forecast.queueingAsyncThreads.retryBackoff == null) {
                        log.error("Required property for GAM Forecasting async queueing control missing: apiconfig.source.google.adManager.forecast.queueingAsyncThreads.retryBackoff");
                        allValid = false;
                    } else {
                        if (gam.forecast.queueingAsyncThreads.retryBackoff.initialIntervalSeconds <= 0) {
                            log.error("Required property for GAM Forecasting async queueing control missing: apiconfig.source.google.adManager.forecast.queueingAsyncThreads.retryBackoff.initialIntervalSeconds");
                            allValid = false;
                        }
                        if (gam.forecast.queueingAsyncThreads.retryBackoff.maxRetryIntervalSeconds <= 0) {
                            log.error("Required property for GAM Forecasting async queueing control missing: apiconfig.source.google.adManager.forecast.queueingAsyncThreads.retryBackoff.maxRetryIntervalSeconds");
                            allValid = false;
                        }
                        if (gam.forecast.queueingAsyncThreads.retryBackoff.totalTimeToWaitMinutes <= 0) {
                            log.error("Required property for GAM Forecasting async queueing control missing: apiconfig.source.google.adManager.forecast.queueingAsyncThreads.retryBackoff.totalTimeToWaitMinutes");
                            allValid = false;
                        }
                    }
                }

                if (gam.forecast.asyncThreads == null) {
                    log.error("Required property for GAM Forecasting parallelism control missing: apiconfig.source.google.adManager.forecast.asyncThreads");
                    allValid = false;
                } else {
                    if (gam.forecast.asyncThreads.requestTimeoutSeconds <= 0) {
                        log.error("Required property for GAM Forecasting request timeout in seconds missing: apiconfig.source.google.adManager.forecast.asyncThreads.requestTimeoutSeconds");
                        allValid = false;
                    }
                    if (gam.forecast.asyncThreads.retryBackoff == null) {
                        log.error("Required property for GAM Forecasting async retry control missing: apiconfig.source.google.adManager.forecast.asyncThreads.retryBackoff");
                        allValid = false;
                    } else {
                        if (gam.forecast.asyncThreads.retryBackoff.initialIntervalSeconds <= 0) {
                            log.error("Required property for GAM Forecasting async retry control missing: apiconfig.source.google.adManager.forecast.asyncThreads.retryBackoff.initialIntervalSeconds");
                            allValid = false;
                        }
                        if (gam.forecast.asyncThreads.retryBackoff.maxRetryIntervalSeconds <= 0) {
                            log.error("Required property for GAM Forecasting async retry control missing: apiconfig.source.google.adManager.forecast.asyncThreads.retryBackoff.maxRetryIntervalSeconds");
                            allValid = false;
                        }
                        if (gam.forecast.asyncThreads.retryBackoff.totalTimeToWaitMinutes <= 0) {
                            log.error("Required property for GAM Forecasting async retry control missing: apiconfig.source.google.adManager.forecast.asyncThreads.retryBackoff.totalTimeToWaitMinutes");
                            allValid = false;
                        }
                    }
                }
            }
        } catch (NullPointerException e) {
            log.error("One or more GAM properties missing from apiconfig.source.gam");
            allValid = false;
        }

        if (allValid)
            log.info("All GAM source properties validated successfully.");

        return allValid;
    }

    /**
     * Validates Facebook configuration details
     *
     * @param facebook
     */
    private Boolean validateFacebook(PipelineConfig.Facebook facebook) {
        Boolean allValid = true;
        PipelineConfig.FacebookContext context;
        try {
            context = facebook.getContext();
            if (context.accessToken.isEmpty()) {
                log.error("Required Property for Facebook source missing: apiconfig.source.facebook.context.accessToken");
                allValid = false;
            };
            if (context.appSecret.isEmpty()) {
                log.error("Required Property for Facebook source missing: apiconfig.source.facebook.context.appSecret");
                allValid = false;
            };
        } catch(NullPointerException e) {
            log.error("Required Property for Facebook source missing: apiconfig.source.facebook.context");
            allValid = false;
        }

        try {
            String id = facebook.getAppId();
            if (id.isEmpty() || id.isBlank()) {
                log.error("Required Property for Facebook source missing: apiconfig.source.facebook.appId");
                allValid = false;
            };
            allValid = true;
        } catch (NullPointerException e) {
            log.error("Required Property for Facebook source missing: apiconfig.source.facebook.appId");
            allValid = false;
        }

        try {
            int sleepIntervalMs = facebook.getSleepIntervalMs();
            if (sleepIntervalMs == 0) {
                log.error("Required Property for Facebook source missing: apiconfig.source.facebook.sleepIntervalMs");
                allValid = false;
            };
            allValid = true;
        } catch (NullPointerException e) {
            log.error("Required Property for Facebook source missing: apiconfig.source.facebook.sleepIntervalMs");
            allValid = false;
        }

        try {
            int maxSleepIntervals = facebook.getMaxSleepIntervals();
            if (maxSleepIntervals == 0) {
                log.error("Required Property for Facebook source missing: apiconfig.source.facebook.maxSleepIntervals");
                allValid = false;
            };
            allValid = true;
        } catch (NullPointerException e) {
            log.error("Required Property for Facebook source missing: apiconfig.source.facebook.maxSleepIntervals");
            allValid = false;
        }

        if (allValid)
            log.info("All Facebook source properties validated successfully.");

        return allValid;
    }

    private Boolean validateTradedesk(PipelineConfig.Tradedesk tradedesk) {
        Boolean allValid = true;
        try {
            if (tradedesk.baseUrl.isEmpty()) {
                log.error("Required property for Tradedesk source missing: apiconfig.source.tradedesk.baseUrl");
                allValid = false;
            }
            if (tradedesk.reportsEndPoint.isEmpty()) {
                log.error("Required property for Tradedesk source missing: apiconfig.source.tradedesk.reportsEndPoint");
                allValid = false;
            }
            if (tradedesk.contractorEndPoint.isEmpty()) {
                log.error("Required property for Tradedesk source missing: apiconfig.source.tradedesk.contractorEndPoint");
                allValid = false;
            }
            if (tradedesk.advertiserEndPoint.isEmpty()) {
                log.error("Required property for Tradedesk source missing: apiconfig.source.tradedesk.advertiserEndPoint");
                allValid = false;
            }
            if (tradedesk.adgroupAdvertiserEndPoint.isEmpty()) {
                log.error("Required property for Tradedesk source missing: apiconfig.source.tradedesk.adgroupAdvertiserEndPoint");
                allValid = false;
            }
            if (tradedesk.partnerId.isEmpty()) {
                log.error("Required property for Tradedesk source missing: apiconfig.source.tradedesk.partnerId");
                allValid = false;
            }
        } catch (NullPointerException e) {
            log.error("One or more Tradedesk properties missing from apiconfig.source.tradedesk");
            allValid = false;
        }


        try {
            Oauth2 auth = tradedesk.getAuthentication();
            if (auth.getOauthUrl().isEmpty()) {
                log.error("Required property for Tradedesk source missing: apiconfig.source.tradedesk.authentication.oauthUrl");
                allValid = false;
            }
            if (auth.getResponseTokenKeyName() == null) {
                log.error("Required property for Tradedesk source missing: apiconfig.source.tradedesk.authentication.accessToken");
                allValid = false;
            }
        } catch (NullPointerException e) {
            log.error("One or more Tradedesk authentication properties missing from apiconfig.source.tradedesk.authentication");
            allValid = false;
        }
        if (allValid)
            log.info("All Tradedesk source properties validated successfully.");

        return allValid;
    }

    private Boolean validateSnapchat(PipelineConfig.SnapChat snapChat) {
        Boolean allValid = true;
        try {
            if (snapChat.baseUrl.isEmpty()) {
                log.error("Required property for SnapChat source missing: apiconfig.source.snapchat.baseUrl");
                allValid = false;
            }
            if (snapChat.version.isEmpty()) {
                log.error("Required property for SnapChat source missing: apiconfig.source.snapchat.version");
                allValid = false;
            }
            if (snapChat.accountId.isEmpty()) {
                log.error("Required property for SnapChat source missing: apiconfig.source.snapchat.accountId");
                allValid = false;
            }
        } catch (NullPointerException e) {
            log.error("One or more SnapChat properties missing from apiconfig.source.snapchat");
            allValid = false;
        }


        try {
            Oauth2 auth = snapChat.getAuthentication();
            Map<String, String> bodyMap = auth.getBodyMap();
            Map<String, String> headersMap = auth.getHeadersMap();
            if (auth.getOauthUrl().isEmpty()) {
                log.error("Required property for SnapChat authentication missing: apiconfig.source.snapchat.authentication.oauthUrl");
                allValid = false;
            }
            if (bodyMap.get("refresh_token").isEmpty()) {
                log.error("Required property for SnapChat authentication missing: apiconfig.source.snapchat.authentication.bodyMap.refresh_token");
                allValid = false;
            }
            if (bodyMap.get("client_id").isEmpty()) {
                log.error("Required property for SnapChat authentication missing: apiconfig.source.snapchat.authentication.bodyMap.client_id");
                allValid = false;
            }
            if (bodyMap.get("client_secret").isEmpty()) {
                log.error("Required property for SnapChat authentication missing: apiconfig.source.snapchat.authentication.bodyMap.client_secret");
                allValid = false;
            }
            if (bodyMap.get("grant_type").isEmpty()) {
                log.error("Required property for SnapChat authentication missing: apiconfig.source.snapchat.authentication.bodyMap.grant_type");
                allValid = false;
            }
            if (headersMap.get("Content-Type").isEmpty()) {
                log.error("Required property for SnapChat authentication missing: apiconfig.source.snapchat.authentication.headersMap.Content-Type");
                allValid = false;
            }
        } catch (NullPointerException e) {
            log.error("One or more SnapChat authentication properties missing from apiconfig.source.snapchat.authentication");
            allValid = false;
        }
        if (allValid)
            log.info("All SnapChat source properties validated successfully.");

        return allValid;
    }

    private Boolean validateS3SwitchBoard(PipelineConfig.S3SwitchBoard s3) {
        Boolean allValid = true;
        try {
            if (s3.bucketName.isEmpty()) {
                log.error("Required property for S3 source missing: apiconfig.source.s3SwitchBoard.bucketName");
                allValid = false;
            }
            if (s3.profileName.isEmpty()) {
                log.error("Required property for S3 source missing: apiconfig.source.s3SwitchBoard.profileName");
                allValid = false;
            }
            if (s3.regionName.isEmpty()) {
                log.error("Required property for S3 source missing: apiconfig.source.s3SwitchBoard.regionName");
                allValid = false;
            }

        } catch (NullPointerException e) {
            log.error("One or more S3 properties missing from apiconfig.source.s3SwitchBoard");
            allValid = false;
        }

        if (allValid)
            log.info("All S3SwitchBoard source properties validated successfully.");

        return allValid;
    }

    private Boolean validatePinterest(PipelineConfig.Pinterest pinterest) {
        Boolean allValid = true;
        try {
            if (pinterest.baseUrl.isEmpty()) {
                log.error("Required property for Pinterest source missing: apiconfig.source.pinterest.baseUrl");
                allValid = false;
            }
            if (pinterest.version.isEmpty()) {
                log.error("Required property for Pinterest source missing: apiconfig.source.pinterest.version");
                allValid = false;
            }
            if (pinterest.baseEndpoint.isEmpty()) {
                log.error("Required property for Pinterest source missing: apiconfig.source.pinterest.baseEndpoint");
                allValid = false;
            }
            if (pinterest.endpointSuffix.isEmpty()) {
                log.error("Required property for Pinterest source missing: apiconfig.source.pinterest.endpointSuffix");
                allValid = false;
            }
        } catch (NullPointerException e) {
            log.error("One or more Pinterest properties missing from apiconfig.source.pinterest");
            allValid = false;
        }

        try {
            Oauth2 auth = pinterest.getAuthentication();
            Map<String, String> headersMap = auth.getHeadersMap();
            Map<String, String> bodyMap = auth.getBodyMap();
            if (headersMap.get("Authorization").isEmpty()) {
                log.error("Required property for Pinterest source missing: apiconfig.source.pinterest.authentication.headersMap.Authorization");
                allValid = false;
            }
            if (bodyMap.get("refresh_token").isEmpty()) {
                log.error("Required property for Pinterest source missing: apiconfig.source.pinterest.authentication.bodyMap.refresh_token");
                allValid = false;
            }
        } catch (NullPointerException e) {
            log.error("One or more Pinterest authentication properties missing from apiconfig.source.pinterest.authentication");
            allValid = false;
        }
        if (allValid)
            log.info("All Pinterest source properties validated successfully.");

        return allValid;
    }

    private Boolean validateCampaignManager360(PipelineConfig.Google.MarketingPlatform googleMarketingPlatform) {
        Boolean allValid = true;
        try {
            if (googleMarketingPlatform.userName.isEmpty()) {
                log.error("Required property for GoogleMarketingPlatform source missing: apiconfig.source.google.marketingPlatform.userName");
                allValid = false;
            }
            if (googleMarketingPlatform.jsonKeyFilePath.isEmpty()) {
                log.error("Required property for GoogleMarketingPlatform source missing: apiconfig.source.google.marketingPlatform.jsonKeyFilePath");
                allValid = false;
            }
        } catch (NullPointerException e) {
            log.error("One or more GoogleMarketingPlatform properties missing from apiconfig.source.google.marketingPlatform");
            allValid = false;
        }

        if (allValid)
            log.info("All GoogleMarketingPlatform source properties validated successfully.");

        return allValid;
    }

    public Boolean validateHdfsDetails(PipelineConfig.Hdfsdetails hdfsdetails) {

        return !CommonUtils.checkIfEmpty(XENONENDPOINT, hdfsdetails.getEndPoint()) && !CommonUtils.checkIfEmpty(XENONFORMAT, hdfsdetails.getFormat());

    }

    /**
     * Validating secrets details
     *
     * @param secrets
     * @return
     */

    public Boolean validateSecrets(@Nullable List<PipelineConfig.Secrets> secrets) {
        Boolean success = true;
        if (secrets == null) {
            log.warn("You havent referenced any secrets");
        } else {
            Boolean flag;
            for (PipelineConfig.Secrets secret : secrets) {
                flag = !CommonUtils.checkIfEmpty(FIELDNAME, secret.getFieldname()) && !CommonUtils.checkIfEmpty(TAPKEY, secret.getTapkey());
                if (!flag)
                    success = false;
            }
        }
        return success;
    }

}
