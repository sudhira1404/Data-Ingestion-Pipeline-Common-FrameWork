package com.target.kelsaapi.common.config;


import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableAsync
@Slf4j
@Lazy
public class AsyncConfiguration implements AsyncConfigurer {

    private final int gamForecastCorePoolSize;

    private final int gamForecastMaxPoolSize;

    private final int gamForecastQueueSize;

    private final int pipelineRunnerCorePoolSize;

    private final int pipelineRunnerMaxPoolSize;

    @Autowired
    public AsyncConfiguration(PipelineConfig config) {
        PipelineConfig.ThreadPool gamForecasThreadPool = config.apiconfig.source.google.adManager.forecast.threadPool;
        this.gamForecastCorePoolSize = gamForecasThreadPool.corePoolSize;
        this.gamForecastMaxPoolSize = gamForecasThreadPool.maxPoolSize;
        this.gamForecastQueueSize = gamForecasThreadPool.queueSize;

        PipelineConfig.ThreadPool pipelineRunnerThreadPool = config.apiconfig.pipelineRunnerListener.threadPool;
        this.pipelineRunnerCorePoolSize = pipelineRunnerThreadPool.corePoolSize;
        this.pipelineRunnerMaxPoolSize = pipelineRunnerThreadPool.maxPoolSize;
    }

    @Bean(name = "gamForecastExecutor")
    public ThreadPoolTaskExecutor gamForecastExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(gamForecastCorePoolSize);
        executor.setMaxPoolSize(gamForecastMaxPoolSize);
        executor.setQueueCapacity(gamForecastQueueSize);
        executor.setThreadNamePrefix("GamForecastExecutor-");
        executor.initialize();
        return executor;
    }

    @Bean(name = "pipelineRunnerListenerExecutor")
    public ThreadPoolTaskExecutor pipelineRunnerListenerExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(pipelineRunnerCorePoolSize);
        executor.setMaxPoolSize(pipelineRunnerMaxPoolSize);
        //Please note this is an unbounded queue, so can grow and use all memory if not careful here.
        //executor.setQueueCapacity(pipelineRunnerQueueSize);
        executor.setThreadNamePrefix("PipelineListenerContainer-");
        executor.initialize();
        return executor;
    }

    @Bean(name = "gamCredentialRefreshMonitor")
    public ThreadPoolTaskExecutor gamCredentialExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.initialize();
        return executor;
    }
}

