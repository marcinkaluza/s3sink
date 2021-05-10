package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.data.Quote;

import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;

public class CustomRollingPolicy extends CheckpointRollingPolicy<Quote, String> {

    private final static int MB = 1024 * 1024;
    private final static int KB = 1024;

    Logger LOG = LoggerFactory.getLogger(CustomRollingPolicy.class);

    @Override
    public boolean shouldRollOnEvent(PartFileInfo<String> partFileInfo, Quote quote) throws IOException {

        if(shouldRoll(partFileInfo)){
            LOG.info("Roll on enabled - file size: {}. Bucket: {}", partFileInfo.getSize(), partFileInfo.getBucketId());
            return true;
        };

        return false;
    }

    @Override
    public boolean shouldRollOnProcessingTime(PartFileInfo<String> partFileInfo, long l) throws IOException {
        if(shouldRoll(partFileInfo)){
            LOG.info("Roll on enabled - file size: {}. Bucket: {}", partFileInfo.getSize(), partFileInfo.getBucketId());
            return true;
        };

        return false;
    }

    private boolean shouldRoll(PartFileInfo<String> partFileInfo) throws IOException {
        return partFileInfo.getSize() > 500 * KB || partFileInfo.getLastUpdateTime() < Instant.now().minusSeconds(300).toEpochMilli();
    }
}
