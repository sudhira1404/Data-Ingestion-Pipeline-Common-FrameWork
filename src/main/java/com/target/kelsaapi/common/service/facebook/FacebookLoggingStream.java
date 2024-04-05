package com.target.kelsaapi.common.service.facebook;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;

@Slf4j
public class FacebookLoggingStream extends OutputStream {

    public static void redirectSysOutAndSysErr() {
        System.setOut(new PrintStream(new FacebookLoggingStream(log, LogLevel.INFO)));
        System.setErr(new PrintStream(new FacebookLoggingStream(log, LogLevel.ERROR)));
    }

    public static void redirectDebug() {
        System.setOut(new PrintStream(new FacebookLoggingStream(log, LogLevel.DEBUG)));
        redirectSysOutAndSysErr();
    }

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);
    private final Logger logger;
    private final LogLevel level;

    public enum LogLevel {
        TRACE, DEBUG, INFO, WARN, ERROR,
    }

    public FacebookLoggingStream(Logger logger, LogLevel level) {
        this.logger = logger;
        this.level = level;
    }

    @Override
    public void write(int b) {
        if (b == '\n') {
            String line = baos.toString();
            baos.reset();

            switch (level) {
                case TRACE:
                    logger.trace(line);
                    break;
                case DEBUG:
                    logger.debug(line);
                    break;
                case ERROR:
                    logger.error(line);
                    break;
                case INFO:
                    logger.info(line);
                    break;
                case WARN:
                    logger.warn(line);
                    break;
            }
        } else {
            baos.write(b);
        }
    }
}
