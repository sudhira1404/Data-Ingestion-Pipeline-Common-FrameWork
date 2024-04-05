package com.target.kelsaapi;

import com.target.platform.connector.config.ConfigSource;
import com.target.platform.connector.config.FileSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Application Entry point
 */
@SpringBootApplication(exclude = {KafkaAutoConfiguration.class})
@EnableRetry
@EnableScheduling
public class Main {
    private static final List<String> fileNames = List.of(
            "source-client.truststore.jks",
            "source-keystore.jks",
            "sink-client.trustore.jks",
            "sink-keystore.jks",
            "error-client.truststore.jks",
            "error-keystore.jks",
            "client_secrets.json",
            "s3credentials",
            "criteo_private_key_file.pem"
    );

    private static final String rootFolderName = "/tmp/";


    /**
     * The main entrypoint for this application. Checks if any of the TAP secrets are defined in the deployment environment.
     * These are specified in the static field fileNames. If any are, it will read the secret from TAP and write it to a
     * local file named the same as the fileName value in the path specified by the static field rootFolderName. Exits
     * with a return code 66 (thank you Clone Wars) if any file fails to be initialized properly. If secret files are
     * initialized, it will then proceed to starting the SpringApplication.
     *
     * @param args Optional command-line arguments
     */
    public static void main(String[] args) {

        fileNames.forEach(name -> {
            FileSource source = ConfigSource.file(name);
            if (source!=null) {
                if(writeSecretFile(source, name, rootFolderName + name)) {
                    //Please note the use of System.xxx.println throughout this method. During startup, these messages
                    //would be generated before the spring framework has initialized. The use of a logger is not possible
                    //here for TAP and docker deployments, since the logger has not yet been initialized.
                    System.out.println("Successfully wrote " + name + " to " + rootFolderName + name);
                } else {
                    System.err.println("Failed to write " + name + " to " + rootFolderName + name);
                    System.exit(66);
                }
            }
        });

        SpringApplication.run(Main.class);
    }

    /**
     * Used to write a TAP secret to a local file.
     *
     * @param sourceValue The TAP secret value
     * @param sourceName The name of the source secret. This will be used as the file name.
     * @param writePath The path to the file named as the sourceName which will have the sourceValue written into.
     * @return True if the file was written to successfully. False if the file failed to be initialized.
     */
    private static boolean writeSecretFile(FileSource sourceValue, String sourceName, String writePath) {
        System.out.println(sourceName + " found in configs or secrets on TAP, will attempt to write to local file " + writePath + " now");
        boolean writeSuccessful = false;
        File writtenFile = new File(writePath);

        try {
            boolean initialized = writtenFile.createNewFile();
            if (initialized && writtenFile.exists() && writtenFile.canWrite()) {
                //Please note the use of System.xxx.println throughout this method. During startup, these messages
                //would be generated before the spring framework has initialized. The use of a logger is not possible
                //here for TAP and docker deployments, since the logger has not yet been initialized.
                System.out.println("File successfully initialized: " + writePath);
            } else {
                throw new SecurityException("File failed to be initialized: " + writePath);
            }
        } catch(SecurityException | IOException e) {
            System.err.println(e.getMessage());
            return writeSuccessful;
        }
        try {
            System.out.println("Attempting to write TAP secret value to initialized file now");
            sourceValue.writeTo(writtenFile);
            writeSuccessful = true;
        } catch(Exception e) {
            System.err.println("Unable to write to local file from platform connector value: " + writePath);
        }
        return writeSuccessful;
    }

}
