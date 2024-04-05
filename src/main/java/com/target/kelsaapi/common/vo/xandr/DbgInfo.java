package com.target.kelsaapi.common.vo.xandr;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.ArrayList;

@Data
public class DbgInfo {

    @JsonProperty("user::read_limit")
    int user_read_limit;

    @JsonProperty("user::write_limit")
    int user_write_limit;

    int read_limit;
    int write_limit;

    @JsonProperty("user::read_limit_seconds")
    int user_read_limit_seconds;

    @JsonProperty("user::write_limit_seconds")
    int user_write_limit_seconds;
    int read_limit_seconds;
    int write_limit_seconds;
    String instance;
    String version;
    double time;
    int start_microtime;
    ArrayList<Object> warnings;
    String api_cache_hit;
    String output_term;
}

