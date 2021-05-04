package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CustomRollingPolicy extends CheckpointRollingPolicy<StockTick, String> {

    Logger LOG = LoggerFactory.getLogger(CustomRollingPolicy.class);

    @Override
    public boolean shouldRollOnEvent(PartFileInfo<String> partFileInfo, StockTick stockTick) throws IOException {
        LOG.info("File size: {}", partFileInfo.getSize());
        return true;
    }

    @Override
    public boolean shouldRollOnProcessingTime(PartFileInfo<String> partFileInfo, long l) throws IOException {
        return true;
    }
}
