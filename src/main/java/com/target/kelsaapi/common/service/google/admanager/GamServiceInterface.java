package com.target.kelsaapi.common.service.google.admanager;

import com.google.api.client.auth.oauth2.Credential;
import com.target.kelsaapi.common.exceptions.GamException;
import com.target.kelsaapi.common.vo.google.request.admanager.GamRequest;
import com.target.kelsaapi.common.vo.google.response.admanager.GamResponse;

/**
 * A factory interface that all Gam Services implement
 */
public interface GamServiceInterface {

    /**
     * Performs a get operation to retrieve a response object from a Gam Service.
     *
     * @param request One of the subclasses of {@link GamRequest}
     * @param credential A refreshed Google {@link Credential} object.
     * @return One of the subclasses of {@link GamResponse}
     * @throws GamException A wrapper exception class containing the message and cause from the underlying exception thrown.
     */
    GamResponse get(GamRequest request, Credential credential) throws GamException;

    /**
     * Performs a get operation to retrieve a response object from a Gam Service.
     *
     * @param credential A refreshed Google {@link Credential} object.
     * @return One of the subclasses of {@link GamResponse}
     * @throws GamException A wrapper exception class containing the message and cause from the underlying exception thrown.
     */
    GamResponse get(Credential credential) throws GamException;

    /**
     * Performs a get operation to retrieve a response object from a Gam Service.
     *
     * @return A refreshed Google {@link Credential} object.
     * @throws GamException A wrapper exception class containing the message and cause from the underlying exception thrown.
     */
    Credential get() throws GamException;

}
