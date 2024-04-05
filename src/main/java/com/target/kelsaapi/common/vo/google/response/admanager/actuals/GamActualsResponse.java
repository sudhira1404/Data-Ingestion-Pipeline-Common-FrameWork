package com.target.kelsaapi.common.vo.google.response.admanager.actuals;

import com.target.kelsaapi.common.vo.google.response.admanager.GamResponse;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.List;

@Slf4j
public class GamActualsResponse extends GamResponse {

    @Getter
    @Setter
    private URL downloadableURL;

    public GamActualsResponse(List<String> gamActuals) {
        super();
        super.setResponseList(gamActuals);
    }

    public GamActualsResponse(URL gamActuals) {
        super();
        this.downloadableURL = gamActuals;
        log.debug("URL to download results from : {}",getDownloadableURL().toExternalForm());
    }

    public GamActualsResponse() {
        super();
    }
}
