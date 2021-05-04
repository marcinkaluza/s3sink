package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;

import java.io.IOException;

public class CustomRollingPolicy extends CheckpointRollingPolicy<StockTick, String> {


    @Override
    public boolean shouldRollOnEvent(PartFileInfo<String> partFileInfo, StockTick stockTick) throws IOException {
        return true;
    }

    @Override
    public boolean shouldRollOnProcessingTime(PartFileInfo<String> partFileInfo, long l) throws IOException {
        return true;
    }
}
