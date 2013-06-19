// Copyright 2012 LinkedIn, Inc

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.linkedin.whiteelephant;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public class TimeZoneConversion
{
  public static java.sql.Timestamp roundTimestampToDay(java.sql.Timestamp timestamp, String timezoneId)
  {
    TimeZone timezone = TimeZone.getTimeZone(timezoneId);    
    Calendar cal = Calendar.getInstance(timezone);
    cal.setTimeInMillis(timestamp.getTime());
    
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    
    return new java.sql.Timestamp(cal.getTimeInMillis());
  }

  public static java.sql.Timestamp roundTimestampToWeek(java.sql.Timestamp timestamp, String timezoneId)
  {
    TimeZone timezone = TimeZone.getTimeZone(timezoneId);    
    Calendar cal = Calendar.getInstance(timezone);
    cal.setTimeInMillis(timestamp.getTime());
    
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    
    return new java.sql.Timestamp(cal.getTimeInMillis());
  }
  
  public static java.sql.Timestamp roundTimestampToMonth(java.sql.Timestamp timestamp, String timezoneId)
  {
    TimeZone timezone = TimeZone.getTimeZone(timezoneId);    
    Calendar cal = Calendar.getInstance(timezone);
    cal.setTimeInMillis(timestamp.getTime());
    
    cal.set(Calendar.DAY_OF_MONTH, 1);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    
    return new java.sql.Timestamp(cal.getTimeInMillis());
  }
  
  public static java.sql.Timestamp roundTimestampToQuarter(java.sql.Timestamp timestamp, String timezoneId)
  {
    TimeZone timezone = TimeZone.getTimeZone(timezoneId);    
    Calendar cal = Calendar.getInstance(timezone);
    cal.setTimeInMillis(timestamp.getTime());
    
    cal.set(Calendar.DAY_OF_MONTH, 1);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);

    // round down to beginning of quarter
    cal.set(Calendar.MONTH, cal.get(Calendar.MONTH) - (cal.get(Calendar.MONTH) % 3));
    
    return new java.sql.Timestamp(cal.getTimeInMillis());
  }
}
