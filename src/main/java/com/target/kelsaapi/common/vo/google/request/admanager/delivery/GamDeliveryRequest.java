package com.target.kelsaapi.common.vo.google.request.admanager.delivery;

import com.google.api.client.util.Lists;
import com.target.kelsaapi.common.vo.google.request.admanager.GamRequest;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class GamDeliveryRequest extends GamRequest {

    private List<String> deliveryResponse;

    GamDeliveryRequest(String startDate, String endDate) {
        super(startDate, endDate);
        this.deliveryResponse = Lists.newArrayList();
    }

}
