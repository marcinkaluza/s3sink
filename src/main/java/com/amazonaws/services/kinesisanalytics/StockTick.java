package com.amazonaws.services.kinesisanalytics;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;

@Data
@NoArgsConstructor
public class StockTick {
    private String isin;
    private DateTime timeStamp;
    private double ask;
    private double bid;
}
