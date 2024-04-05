package com.target.kelsaapi.common.service.postgres.s3;

import com.target.kelsaapi.common.config.JdbcConfig;
import com.target.kelsaapi.common.exceptions.NotSupportedException;
import com.target.kelsaapi.common.util.textFormatterInterface;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;

@Service
@Slf4j
public class S3GetPrefixService implements textFormatterInterface {

    private final JdbcConfig config;

    @Autowired
    S3GetPrefixService(JdbcConfig jdbcConfig) {
        this.config = jdbcConfig;
    }


    public String getKeyNamePath(String prefix,String startDate ) throws SQLException, NotSupportedException {

        String derivedPrefix = prefix;
        Connection connect = DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
        String firstStringInPrefix = prefix.trim().substring(0, prefix.indexOf(' '));
        if (firstStringInPrefix.toUpperCase().contains("SELECT") &&  !firstStringInPrefix.toUpperCase().contains("WHERE")) {
            String sqlPrefix = prefix.replaceAll("\\$startDate", startDate);
            try (Connection conn = connect;
                 PreparedStatement p = conn.prepareStatement(sqlPrefix))
                {
                try (ResultSet rs = p.executeQuery()) {
                    rs.next();
                    derivedPrefix = rs.getString(1);
                    return derivedPrefix;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
        else {log.info("Prefix parameter provided is not a select query or select contains where clause ,so will not be executed:" + prefix);
            throw new NotSupportedException(ANSI_RED + "Prefix parameter provided is not a select query or select contains where clause,so will not be executed.Provide a select query" +
                    " which evaluates to a prefix path when query is executed to continue the flow" + ANSI_RESET);

        }
        return derivedPrefix;
    }




}