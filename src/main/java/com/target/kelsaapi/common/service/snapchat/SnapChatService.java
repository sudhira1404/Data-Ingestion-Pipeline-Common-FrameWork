package com.target.kelsaapi.common.service.snapchat;

import com.target.kelsaapi.common.exceptions.SnapChatException;
import com.target.kelsaapi.common.vo.Oauth;

import java.util.ArrayList;

public interface SnapChatService {
    ArrayList<String> getSnapChatCampaignDetails(Oauth oauth) throws SnapChatException;
    ArrayList<String> getSnapChatCampaignStats(Oauth oauth, String startDate, String endDate) throws SnapChatException;
}