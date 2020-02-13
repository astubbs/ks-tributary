package io.confluent.ps.streams.referenceapp.chat.streams;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.joda.time.DateTime;

import java.time.MonthDay;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

// TODO delete? not used? doesnt work?
public class YearlyWindow extends Windows<TimeWindow>  {

  private MonthDay startingMonthDay;
  private TimeZone timezone;

  public YearlyWindow(int month, int day) {
    TimeZone gmt = TimeZone.getTimeZone("GMT"); // customers timezone is GMT
    timezone = gmt;

    Calendar cal = Calendar.getInstance(this.timezone);
    cal.set(2020, month, day);
    this.startingMonthDay = MonthDay.of(month, day);//fromCalendarFields(cal);
  }

  @Override
  public Map<Long, TimeWindow> windowsFor(long timestamp) {
    DateTime dateTime = new DateTime(timestamp);

    int year = dateTime.getYear();
    long start = getStartForYear(year);

    DateTime endDate = dateTime.plusYears(1);
    long endExclusive = endDate.toInstant().getMillis();

    TimeWindow timeWindow = new TimeWindow(start, endExclusive);
    Map<Long, TimeWindow> windowMap = new HashMap<>();
    windowMap.put(start, timeWindow);

    return windowMap;
  }

  private long getStartForYear(int year) {
    Calendar calendar = Calendar.getInstance(this.timezone);
    calendar.set(year, startingMonthDay.getMonthValue(), startingMonthDay.getDayOfMonth());
    long start = calendar.toInstant().toEpochMilli();
    return start;
  }

  @Override
  public long size() {
    throw new NotImplementedException("");
  }

  @Override
  public long gracePeriodMs() {
    throw new NotImplementedException("");
  }
}
