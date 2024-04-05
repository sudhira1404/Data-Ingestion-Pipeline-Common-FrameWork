package com.target.kelsaapi.common.vo.google.request.admanager;

import com.google.api.ads.admanager.axis.v202311.Date;
import lombok.Data;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

@Data
public abstract class GamRequest {

    protected String startDate;

    protected String endDate;

    public GamRequest() {
        this.startDate = null;
        this.endDate = null;
    }

    public GamRequest(String startDate, String endDate) {
        this.startDate = startDate;
        this.endDate = endDate;
    }

    protected Date toDate(String date) throws ParseException {
        Calendar cal = new GregorianCalendar();
        java.util.Date simpleDate = new SimpleDateFormat("yyyy-MM-dd").parse(date);
        cal.setTime(simpleDate);

        Date newDate = new Date();
        newDate.setYear(cal.get(Calendar.YEAR));
        newDate.setMonth(cal.get(Calendar.MONTH) + 1);
        newDate.setDay(cal.get(Calendar.DAY_OF_MONTH));

        return newDate;
    }
}
