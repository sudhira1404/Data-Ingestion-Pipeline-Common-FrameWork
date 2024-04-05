package com.target.kelsaapi.common.vo.google.response.admanager;

import com.google.api.client.util.Lists;
import lombok.Data;

import java.util.List;

@Data
public abstract class GamResponse {

    private List<String> responseList;

    protected GamResponse() {
        this.responseList = Lists.newArrayList();
    }

    protected GamResponse(List<String> response) {
        this.responseList = response;
    }

}
