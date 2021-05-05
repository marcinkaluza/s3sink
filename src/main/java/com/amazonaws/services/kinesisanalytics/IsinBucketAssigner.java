
package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.data.StockTick;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class IsinBucketAssigner implements BucketAssigner<StockTick,String> {

    public IsinBucketAssigner() {
    }

    @Override
    public String getBucketId(StockTick stockTick, Context context) {

        var id = String.format("%s/%d-%02d-%02d",
                stockTick.getIsin(),
                stockTick.getTimeStamp().getYear(),
                stockTick.getTimeStamp().getMonthOfYear(),
                stockTick.getTimeStamp().getDayOfMonth());
        return id;
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return new SimpleVersionedSerializer<>() {
            @Override
            public int getVersion() {
                return 0;
            }

            @Override
            public byte[] serialize(String s) throws IOException {
                return s.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public String deserialize(int i, byte[] bytes) throws IOException {
                return new String(bytes);
            }
        };
    }
}
