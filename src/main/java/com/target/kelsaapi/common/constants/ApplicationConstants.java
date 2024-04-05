package com.target.kelsaapi.common.constants;

import lombok.Getter;

/**
 * Application constants
 * @since 1.0
 */
public interface ApplicationConstants {
    String ACCEPT = "Accept";
    String XENONRESPONSE ="application/x-get-service-info+json";
    String TEMPFILE = "tmp.txt";
    String SERVERS = "servers";
    String PART="part";
    String CHILDREN="children";
    String TYPE="type";
    String DIR = "dir";
    String XENONWRITEPARAM = "?overwrite=%s&mkdirs=%s&append=%s";
    String NAME = "name";
    String CONTENTTYPE = "Content-Type"; // header used to declare content type
    String JSONDATA = "application/json";
    String BEARER = "Bearer ";
    String ACCESSTOKEN = "access_token";
    String USERNAME = "username";
    String QUESTIONMARK = "?";
    String EQUAL = "=";
    String HDFS = "hdfs";//hdfs sink type
    String FIELDNAME = "field name";
    String TAPKEY = "tap key";
    String XENONENDPOINT = "Xenon endpoint";
    String XENONFORMAT = "Xenon Format";
    String PIPELINE_LOGGER_NAME = "pipelineRunId";

    int DEFAULT_MAX_WRITE_ATTEMPTS = 3;

    String TEMP_FOLDER_ROOT_NAME = "data1";

    enum slackMessageType {
        SUCCESS,
        FAILURE
    }

    String TEXT = "text";
    String CHANNEL = "channel";

    enum Sources {
        CAMPAIGN_MANAGER_360,
        FACEBOOK,
        GAM,
        PINTEREST,
        SNAPCHAT,
        TRADEDESK,
        TRADEDESKAPI,
        S3,
        SALESFORCE,
        XANDR,
        CRITEO,
        INDEXEXCHANGE
    }

    enum PipelineStates {
        INITIALIZED,
        RUNNING,
        FAILED,
        COMPLETED
    }


    enum CampaignManager360ReportTypes {
        CAMPAIGN (FileExtensions.CSV_GZ.getName());

        @Getter
        private final String fileExtension;

        CampaignManager360ReportTypes(String fileExt) {
            this.fileExtension = fileExt;
        }
    }

    enum SnapChatReportTypes {
        STATS (FileExtensions.JSON_GZ.getName()),
        CAMPAIGNS (FileExtensions.JSON_GZ.getName());

        @Getter
        private final String fileExtension;

        SnapChatReportTypes(String fileExt) {
            this.fileExtension = fileExt;
        }
    }

    enum GamReportTypes {
        ACTUALS(FileExtensions.CSV_TAR_GZ.getName()),
        DELIVERY(FileExtensions.JSON_TAR_GZ.getName()),
        FORECAST(FileExtensions.JSON_TAR_GZ.getName());

        @Getter
        private final String fileExtension;

        GamReportTypes(String fileExt) {
            this.fileExtension = fileExt;
        }
    }

    enum FacebookReportTypes {
        CAMPAIGN_INSIGHTS (FileExtensions.JSON_GZ.getName());

        @Getter
        private final String fileExtension;

        FacebookReportTypes(String fileExt) {
            this.fileExtension = fileExt;
        }
    }

    enum PinterestReportTypes {
        CAMPAIGN (FileExtensions.CSV_TAR_GZ.getName()),
        AUDIENCE (FileExtensions.JSON_GZ.getName());

        @Getter
        private final String fileExtension;

        PinterestReportTypes(String fileExt) {
            this.fileExtension = fileExt;
        }
    }

    enum SalesforceReportTypes {
        ACCOUNT (FileExtensions.JSON_GZ.getName()),
        BRAND__C (FileExtensions.JSON_GZ.getName()),
        OPPORTUNITY (FileExtensions.JSON_GZ.getName()),
        BRAND_AGENCY_ASSIGNMENT__C (FileExtensions.JSON_GZ.getName()),
        BRAND_OPPORTUNITY__C (FileExtensions.JSON_GZ.getName()),
        CAMPAIGN__C (FileExtensions.JSON_GZ.getName()),
        CAMPAIGN_PRODUCT__C (FileExtensions.JSON_GZ.getName()),
        CLASS__C (FileExtensions.JSON_GZ.getName()),
        CONTACT (FileExtensions.JSON_GZ.getName()),
        REVENUE__C (FileExtensions.JSON_GZ.getName()),
        USER (FileExtensions.JSON_GZ.getName()),
        VENDOR_NUMBER__C (FileExtensions.JSON_GZ.getName());
        @Getter
        private final String fileExtension;

        SalesforceReportTypes(String fileExt) {
            this.fileExtension = fileExt;
        }
    }

    enum TradedeskReportTypes {
        ACTUALS (FileExtensions.CSV_GZ.getName(),10354858),
        OPTIMUS_PERFORMANCE (FileExtensions.CSV_GZ.getName(), 8140858),
        OPTIMUS_AVAILABILITY (FileExtensions.CSV_GZ.getName(), 8140871),
        OPTIMUS_ADGROUP_CONTRACT (FileExtensions.CSV_GZ.getName(), 28443687);

        @Getter
        private final String fileExtension;

        @Getter
        private final int reportId;

        TradedeskReportTypes(String fileExt, int reportId) {

            this.fileExtension = fileExt;
            this.reportId = reportId;
        }
    }

    enum TradedeskApiReportTypes {

        OPTIMUS_CONTRACT_FLOOR_PRICE_API (FileExtensions.JSON_GZ.getName()),

        OPTIMUS_ADGROUP_BID_API (FileExtensions.JSON_GZ.getName());
        @Getter
        private final String fileExtension;

        TradedeskApiReportTypes(String fileExt) {
            this.fileExtension = fileExt;
        }
    }

    enum SftpReportTypes {

        CRITEO_CAMPAIGN_DLY_SNAPSHOT (FileExtensions.TSV.getName()),

        CRITEO_KEYWORD_DLY_SNAPSHOT (FileExtensions.TSV.getName());
        @Getter
        private final String fileExtension;

        SftpReportTypes(String fileExt) {
            this.fileExtension = fileExt;
        }
    }
    enum XandrReportTypes {

        CURATORANALYTICS (FileExtensions.CSV_GZ.getName()),
        PUBLISHERPERFORMANCE (FileExtensions.CSV_GZ.getName()),
        TASELLER (FileExtensions.CSV_GZ.getName());

        @Getter
        private final String fileExtension;

        XandrReportTypes(String fileExt) {
            this.fileExtension = fileExt;
        }
    }
    enum IndexExchangeReportTypes {

        DEALMETRICS(FileExtensions.CSV_GZ.getName(), 2878891);
        @Getter
        private final String fileExtension;
        @Getter
        private final int reportId;

        IndexExchangeReportTypes(String fileExt, int reportId) {
            this.fileExtension = fileExt;
            this.reportId = reportId;
        }
    }

    enum FileExtensions {
        JSON_GZ (".json.gz"),
        JSON (".json"),
        JSON_TAR (".json.tar"),
        JSON_TAR_GZ (".json.tar.gz"),
        CSV_GZ (".csv.gz"),
        CSV (".csv"),
        CSV_TAR (".csv.tar"),
        CSV_TAR_GZ (".csv.tar.gz"),
        TAR_GZ(".tar.gz"),
        TAR(".tar"),
        TSV (".tsv"),
        TSV_GZ (".tsv.gz");

        @Getter
        private final String name;

        FileExtensions(String name) {
            this.name = name;
        }

    }

    String EMPTY_POSTAL_CODE_GAM_QUERY = "where POSTAL_CODE = ''";

    enum GamForecastTypes {
        AVAILABILITY,
        DELIVERY;
    }


    enum S3ReportTypes {
        CAMPAIGN_MANAGER_360CAMPAIGN(S3Prefix.CAMPAIGN_MANAGER_360CAMPAIGN.getPrefixPath(),false,false,false,true,false,false,0,false,false);



        @Getter
        private final String s3Prefix;

        @Getter
        private final Boolean dirDownload;


        @Getter
        private final Boolean xferMgrSingleFileDownload;

        @Getter
        private final Boolean s3PrefixSqlFunction;

        @Getter
        private final Boolean compareWithPrevLoadedFilesCheckByCurrentDate;

        @Getter
        private final Boolean compareWithPrevLoadedFilesCheck;

        @Getter
        private final Boolean abortNoFileFound;

        @Getter
        private final Integer fileAge;

        @Getter
        private final Boolean splitfile;

        @Getter
        private final Boolean splitfilecompress;

        S3ReportTypes(String s3Prefix,Boolean dirDownload,Boolean xferMgrSingleFileDownload,
                      Boolean s3PrefixSqlFunction,Boolean compareWithPrevLoadedFilesCheckByCurrentDate,Boolean compareWithPrevLoadedFilesCheck,
                      Boolean abortNoFileFound,Integer fileAge,Boolean splitfile,Boolean splitfilecompress) {

            this.s3Prefix = s3Prefix;
            this.dirDownload = dirDownload;
            this.xferMgrSingleFileDownload = xferMgrSingleFileDownload;
            this.s3PrefixSqlFunction = s3PrefixSqlFunction;
            this.compareWithPrevLoadedFilesCheck = compareWithPrevLoadedFilesCheck;
            this.compareWithPrevLoadedFilesCheckByCurrentDate = compareWithPrevLoadedFilesCheckByCurrentDate;
            this.abortNoFileFound=abortNoFileFound;
            this.fileAge = fileAge;
            this.splitfile = splitfile;
            this.splitfilecompress = splitfilecompress;

        }
    }


    enum S3Prefix {

        CAMPAIGN_MANAGER_360CAMPAIGN ("target/google_campaign_manager/daily_report/date=");

        @Getter
        private final String prefixPath;

        S3Prefix(String prefixPath) {
            this.prefixPath = prefixPath;
        }

    }

}
