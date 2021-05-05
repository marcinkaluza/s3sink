package com.amazonaws.services.kinesisanalytics;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;
import org.joda.time.Instant;



@Data
@NoArgsConstructor
public class StockTick {
    private String isin;
    @JsonDeserialize(using = TimestampDeserializer.class)
    private long timeStamp;
    private double ask;
    private double bid;

    public String getTimeStampAsString(){
        var dateTime = new DateTime(timeStamp);
        return dateTime.toString("yyyy-MM-dd'T'HH:mm:ss'Z'");
    }

    public String getBucketKey() {
        var dateTime = new DateTime(timeStamp);
        return isin + "/" + dateTime.toString("yyyy-MM-dd");
    }
}
