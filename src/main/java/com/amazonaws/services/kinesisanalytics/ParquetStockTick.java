package com.amazonaws.services.kinesisanalytics;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ParquetStockTick {
    private String isin;
    private String timeStamp;

    private double ask;
    private double bid;
}
