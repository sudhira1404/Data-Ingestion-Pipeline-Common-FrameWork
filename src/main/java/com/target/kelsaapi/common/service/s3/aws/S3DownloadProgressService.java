package com.target.kelsaapi.common.service.s3.aws;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.target.kelsaapi.common.util.textFormatterInterface;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3DownloadProgressService implements textFormatterInterface {
    public static void waitForCompletion(Transfer xfer) {
        try {
            xfer.waitForCompletion();
        } catch (AmazonServiceException e) {
            log.error(ANSI_RED + "Amazon service error: " + e.getMessage() + ANSI_RESET);
            System.exit(1);
        } catch (AmazonClientException e) {
            log.error(ANSI_RED + "Amazon client error: " + e.getMessage() + ANSI_RESET);
            System.exit(1);
        } catch (InterruptedException e) {
            log.error(ANSI_RED + "Transfer interrupted: " + e.getMessage() + ANSI_RESET);
            System.exit(1);
        }
    }

    public static void showTransferProgress(Transfer xfer) {
        log.info(xfer.getDescription());
        printProgressBar(0.0);
        do {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }
            TransferProgress progress = xfer.getProgress();
            long so_far = progress.getBytesTransferred();
            long total = progress.getTotalBytesToTransfer();
            double pct = progress.getPercentTransferred();
            eraseProgressBar();
            printProgressBar(pct);
        } while (xfer.isDone() == false);
        TransferState xfer_state = xfer.getState();
        log.info(": " + xfer_state);
    }


    public static void printProgressBar(double pct) {
        final int bar_size = 40;
        final String empty_bar = "                                        ";
        final String filled_bar = "########################################";
        int amt_full = (int) (bar_size * (pct / 100.0));

        System.out.format("  [%s%s]", filled_bar.substring(0, amt_full),
                empty_bar.substring(0, bar_size - amt_full));

    }

    public static void eraseProgressBar() {
        final String erase_bar = "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b";
        System.out.format(erase_bar);
    }

}
