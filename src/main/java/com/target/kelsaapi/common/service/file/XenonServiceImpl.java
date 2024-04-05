package com.target.kelsaapi.common.service.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.NotFoundException;
import com.target.kelsaapi.common.exceptions.ReaderException;
import com.target.kelsaapi.common.exceptions.WriterException;
import com.target.kelsaapi.common.service.rest.HttpService;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Service to interact with bigred using Xenon client
 *
 * @Author Sonali.Polaki
 * @Since 1.0
 */
@Slf4j
@Service("xenonService")
public class XenonServiceImpl implements XenonService {
    
    private final ObjectMapper objectMapper;
    //TODO replace restTemplate with HttpService in all the methods
    //TODO Add the documentation for all the methods, parameter definition ,version details examples of using api
    //TODO Dont hard code the strings
    //TODO Revisit the input parameters for all the methods
    //TODO Return type should say whether the operation was failure or success
    //TODO Remove the Batch param
    //TODO throw the AuthenticationException
    //@Qualifier("httpService")
    //private final HttpService httpService;

    private final RestTemplate restTemplate;
    
    private final PipelineConfig.Hdfsdetails hdfsdetails;

    @Autowired
    XenonServiceImpl(RestTemplate restTemplate, PipelineConfig config) {
        //this.httpService = httpService;
        this.restTemplate = restTemplate;
        this.objectMapper = new ObjectMapper();
        this.hdfsdetails = config.getApiconfig().hdfsdetails;
    }

    public XenonServiceImpl(HttpService httpService, RestTemplate restTemplate, PipelineConfig.Hdfsdetails hdfsdetails) {
        //this.httpService = httpService;
        this.restTemplate = restTemplate;
        this.objectMapper = new ObjectMapper();
        this.hdfsdetails = hdfsdetails;
    }


    /**
     * @return headers
     */
    private HttpHeaders createHeaders() {
        HttpHeaders headers = new HttpHeaders();
        log.debug("Username from PipeLineConfigs: " + hdfsdetails.getUserName());
        headers.setBasicAuth(hdfsdetails.getUserName(), hdfsdetails.getPassword());
        headers.add(ApplicationConstants.ACCEPT, ApplicationConstants.XENONRESPONSE);
        log.debug("Headers: " + headers.toString());
        return headers;
    }

    /**
     * @return list of endpoints for xenon
     */
    @Override
    public String getXenonNode() throws NullPointerException {
        List<String> nodes = new ArrayList<>();

        log.info("Getting Xenon nodes");
        // create request
        HttpEntity<?> httpEntity = new HttpEntity<>(createHeaders());
        // make a request
        log.debug("Xenon endpoint is " + hdfsdetails.getEndPoint());

        ResponseEntity<JsonNode> response = restTemplate.exchange(hdfsdetails.getEndPoint(), HttpMethod.OPTIONS, httpEntity, JsonNode.class);
        // get JSON response
        HttpStatus status = (HttpStatus) response.getStatusCode();
        log.debug(String.valueOf(response.getStatusCode().value()));
        log.debug("response is: " + response.getBody());
        for (final JsonNode server : Objects.requireNonNull(response.getBody()).get(ApplicationConstants.SERVERS)) {
            nodes.add(server.textValue());
        }
        int maxSize = nodes.size();
        Random random = new Random();
        String node = nodes.get(random.nextInt(maxSize));
        log.info("Connecting to node: " + node);


        return node;
    }


    /**
     * @param path        - Details for batch api
     * @return List of details present in the file
     * @throws ReaderException
     */
    @Override
    public List<String> readFile(String path) throws ReaderException {
        List<String> inputQueryArgumentsList = new ArrayList<>(1);
        try {
            //get xenon nodes
            String xenonNode = getXenonNode();

            log.info("connecting to the node" + xenonNode + "in the path" + path);
            // create request
            HttpEntity<?> request = new HttpEntity<>(createHeaders());
            String xenonFormattedAPI = String.format(hdfsdetails.getFormat(), xenonNode, path);
            log.info("xenonFormattedAPI is " + xenonFormattedAPI);
            log.info("Request is made to" + xenonFormattedAPI);
            ResponseEntity<String> fileContents = restTemplate.exchange(xenonFormattedAPI, HttpMethod.GET, request, String.class);

            if (fileContents.getStatusCode().value() == 200) {
                log.info("Able to read the file contents successfully ");
                log.debug("Contents are :" + fileContents.getBody());
                inputQueryArgumentsList = Arrays.asList(Objects.requireNonNull(fileContents.getBody()).split("\n"));
            }


        } catch (Exception e) {
            log.error("Error in reading from the path :" + path, e);
            throw new ReaderException("Error in reading from the path :" + path, e);

        }
        return inputQueryArgumentsList;
    }

    /**
     * Transfers local file to target file on remote HDFS system.
     *
     * @param filePath The target file on the remote HDFS system to transfer to.
     * @param tempFile The local file to transfer from.
     * @param overwrite True to overwrite the target file on the remote HDFS system; false to not overwrite.
     * @param mkdirs True to create any missing directories on the remote HDFS system; false to not create directories.
     * @param append True to append to an existing target file on the remote HDFS system; false to not append.
     * @return True if file transfer was successful; false if not successful.
     */
    @Override
    public Boolean transferFile(String filePath, String tempFile, Boolean overwrite, Boolean mkdirs, Boolean append) {
        try {
            //get xenon nodes
            String xenonNode = getXenonNode();

            // create request
            HttpEntity<FileSystemResource> requestEntity = new HttpEntity<>(new FileSystemResource(tempFile), createHeaders());
            String formatString = hdfsdetails.getFormat() + ApplicationConstants.XENONWRITEPARAM;
            String xenonFormattedAPI = String.format(formatString, xenonNode, filePath, overwrite, mkdirs, append);

            // make a request
            log.info("Beginning file transfer of {} to: {}", tempFile, filePath);
            ResponseEntity<String> fileUpload = restTemplate.exchange(xenonFormattedAPI, HttpMethod.PUT, requestEntity, String.class);
            HttpStatus status = (HttpStatus) fileUpload.getStatusCode();
            log.debug("Status code: {}", status.value());
            log.debug("Status reason phrase: {}", status.getReasonPhrase());
            log.debug("response is: " + fileUpload.getBody());

            int statusCode = status.value();
            // When creating a new file, Xenon API responds with a 201 HTTP Status Code.
            // When overwriting an existing file, Xenon API responds with a 204 HTTP Status Code
            if (Integer.valueOf(201).equals(statusCode) || Integer.valueOf(204).equals(statusCode)) {
                log.info("File transferred: " + filePath);
                return true;
            } else {
                throw new IOException(String.format("Failure %s in HTTP response was: %s",status, status.getReasonPhrase()));
            }
        } catch (Exception e) {
            log.error("Could not write to the file :" + filePath, e);
            return false;
        }
    }


    /**
     * @param filePath    - Path of the file you want to be deleted from bigred
     * @return true if the file was successfully deleted
     */
    @Override
    public Boolean deleteFile(String filePath) throws WriterException {
        boolean success = false;
        try {   //get xenon nodes
            String xenonNode = getXenonNode();
            // create request
            HttpEntity<?> request = new HttpEntity<>(createHeaders());
            RequestCallback requestCallback = restTemplate.httpEntityCallback(request);
            String xenonFormattedAPI = String.format(hdfsdetails.getFormat(), xenonNode, filePath);

            String result = restTemplate.execute(xenonFormattedAPI, HttpMethod.DELETE, requestCallback, clientHttpResponse -> filePath + "deleted");

            log.info(result);
            success = true;
        } catch (Exception e) {
            log.error("File could not be deleted");
            throw new WriterException("File could not be deleted", e);
        }

        return success;
    }

    /**
     * Checks file on remote HDFS system to detect whether it exists or not.
     *
     * @param filePath The file path on the remote HDFS system to check.
     * @return true if the file exists
     */
    @Override
    public Boolean isFileExists(String filePath) {
        Boolean exists = false;
        try {

            //get xenon nodes
            String xenonNode = getXenonNode();
            // create request
            HttpEntity<?> request = new HttpEntity<>(createHeaders());
            RequestCallback requestCallback = restTemplate.httpEntityCallback(request);
            String xenonFormattedAPI = String.format(hdfsdetails.getFormat(), xenonNode, filePath);

            exists = restTemplate.execute(xenonFormattedAPI, HttpMethod.GET, requestCallback, clientHttpResponse -> true);
        } catch (Exception e) {
            log.error("File not found at" + filePath);
            exists = false;

        }
        return exists;
    }

    /**
     * @param folderPath  - Path of the folder you want to create in bigred
     * @return true if thr folder was created successfully
     */

    @Override
    public Boolean createFolder(String folderPath) throws WriterException {
        Boolean success = true;
        try {

            //get xenon nodes
            String xenonNode = getXenonNode();
            // create request
            HttpEntity<FileSystemResource> requestEntity = new HttpEntity<>(createHeaders());
            String xenonFormattedAPI = String.format(hdfsdetails.getFormat() + ApplicationConstants.QUESTIONMARK + ApplicationConstants.TYPE + ApplicationConstants.EQUAL
                    + ApplicationConstants.DIR, xenonNode, folderPath);

            // make a request
            ResponseEntity<JsonNode> response = restTemplate.exchange(xenonFormattedAPI, HttpMethod.PUT, requestEntity, JsonNode.class);
            log.info(folderPath + "created");
            success = true;

        } catch (Exception e) {
            log.error("Folder could not be created" + e.getMessage());
            success = false;
            throw new WriterException("Folder could not be created at " + folderPath);

        }
        return success;
    }

    /**
     * List contents of the folder
     *
     * @param folderPath  - Path of the folder in bigred
     * @return List of names of files or folders in the input folder
     */
    @Override
    public List<String> readFolder(String folderPath) throws ReaderException {
        List<String> result = new ArrayList<>();
        try {

            //get xenon nodes
            String xenonNode = getXenonNode();
            // create request
            HttpEntity<List<String>> request = new HttpEntity<>(createHeaders().remove(ApplicationConstants.ACCEPT));
            RequestCallback requestCallback = restTemplate.httpEntityCallback(request);
            String xenonFormattedAPI = String.format(hdfsdetails.getFormat() + ApplicationConstants.QUESTIONMARK + ApplicationConstants.PART + ApplicationConstants.EQUAL
                    + ApplicationConstants.CHILDREN, xenonNode, folderPath);

            // make a request

            return restTemplate.execute(xenonFormattedAPI, HttpMethod.GET, requestCallback,clientHttpResponse -> {
                String jsonString = StreamUtils.copyToString(clientHttpResponse.getBody(), StandardCharsets.UTF_8);

                JsonNode main = objectMapper.readValue(jsonString, JsonNode.class);
                Iterator<JsonNode> ListOfiles = main.get(ApplicationConstants.CHILDREN).elements();
                while (ListOfiles.hasNext()) {
                    result.add(ListOfiles.next().get(ApplicationConstants.NAME).asText());
                }
                return result;

            });
        } catch (Exception e) {
            log.error("Folder could not be read" + folderPath);
            throw new ReaderException("Folder could not be read", e);
        }

    }

    /**
     * @param folderPath  - Path of the folder you want to delete
     * @return true if the folder was deleted successfully
     */

    @Override
    public Boolean deleteFolder(String folderPath) throws WriterException {


        Boolean success = true;

        try {   //get xenon nodes
            String xenonNode = getXenonNode();
            // create request
            HttpEntity<?> request = new HttpEntity<>(createHeaders());
            RequestCallback requestCallback = restTemplate.httpEntityCallback(request);
            String xenonFormattedAPI = String.format(hdfsdetails.getFormat(), xenonNode, folderPath);

            String result = restTemplate.execute(xenonFormattedAPI, HttpMethod.DELETE, requestCallback, clientHttpResponse -> folderPath + " deleted");
            log.info(result);
            success = true;
        } catch (Exception e) {

            log.error("Folder could not be deleted");
            throw new WriterException("Folder could not be deleted", e);

        }
        return success;
    }


    /**
     * @param folderPath  - Path of the folder you want to check if it exist
     * @return True if the folder exists, false otherwise
     */
    @Override
    public Boolean isFolderExists(String folderPath) throws NotFoundException {
        Boolean exists = false;
        try {
            //get xenon nodes
            String xenonNode = getXenonNode();
            // create request
            HttpHeaders headers = createHeaders();
            headers.remove("Accept");
            HttpEntity<?> request = new HttpEntity<>(headers);
            RequestCallback requestCallback = restTemplate.httpEntityCallback(request);
            String formatString = hdfsdetails.getFormat() + ApplicationConstants.QUESTIONMARK +
                    ApplicationConstants.PART + ApplicationConstants.EQUAL + ApplicationConstants.CHILDREN;
            String xenonFormattedAPI = String.format(formatString, xenonNode, folderPath);
            exists = restTemplate.execute(xenonFormattedAPI, HttpMethod.GET, requestCallback, clientHttpResponse -> true);
        } catch (Exception e) {
            log.error("Folder does not exist");
            exists = false;
            throw new NotFoundException("Folder does not exist", e);
        }
        return exists;

    }

}