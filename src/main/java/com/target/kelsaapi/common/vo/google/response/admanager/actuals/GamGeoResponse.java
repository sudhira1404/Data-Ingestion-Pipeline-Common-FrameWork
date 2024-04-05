package com.target.kelsaapi.common.vo.google.response.admanager.actuals;

import com.google.api.client.util.Lists;
import com.target.kelsaapi.common.vo.google.response.admanager.GamResponse;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public class GamGeoResponse extends GamResponse {

    private String HEADER = "name";

    private String MISSING_ZIP_CODE = "N/A";

    private List<String> zipCodes;

    private List<String[]> zipCodeArray;

    public GamGeoResponse(List<String[]> zipCodes) {

        super();

        this.zipCodeArray = zipCodes;

        setZipCodes();

        setResponseList(dedupeZipCodes());

    }

    private void setZipCodes() {
        this.zipCodes = Lists.newArrayList();

        zipCodes.add(MISSING_ZIP_CODE);

        for (String[] code : zipCodeArray) {
            String zipName = code[0];
            if (Boolean.FALSE.equals(zipName.equals(HEADER))) {
                zipCodes.add(code[0]);
            } else {
                log.debug("Removed header {} from the list of zip codes",HEADER);
            }
        }
    }

    private List<String> dedupeZipCodes() {
        return zipCodes.stream().distinct().map(name -> ("'" + name + "'")).collect(Collectors.toList());
    }
}
