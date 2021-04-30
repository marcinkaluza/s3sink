package com.amazonaws.services.kinesisanalytics;

import lombok.Data;

import java.time.Instant;
import java.util.Date;

@Data
public class StockTick {
    private String isin;
    private Date timeStamp;
    private double ask;
    private double bid;
}
