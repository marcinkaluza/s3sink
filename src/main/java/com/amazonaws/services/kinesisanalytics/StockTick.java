package com.amazonaws.services.kinesisanalytics;

import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;

@Data
@NoArgsConstructor
public class StockTick {
    private String isin;
    private Date timeStamp;
    private double ask;
    private double bid;
}
