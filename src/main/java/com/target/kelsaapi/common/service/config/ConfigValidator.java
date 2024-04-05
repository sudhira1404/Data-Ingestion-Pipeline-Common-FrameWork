package com.target.kelsaapi.common.service.config;

import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.pipelines.config.PipelineConfig;


public interface ConfigValidator {

    public Boolean validateConfig(PipelineConfig pipelineConfig) throws ConfigurationException;
}
