package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.data.StockTick;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class S3StreamingSinkJob {
    private static final String region = "eu-west-1";
    private static final String inputStreamName = "stockprices";

    private static final Logger LOG = LoggerFactory.getLogger(S3StreamingSinkJob.class);

    private static DataStream<StockTick> createSource(StreamExecutionEnvironment env) {

        LOG.debug("Creating source...");

        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION,
                "LATEST");
        inputProperties.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "1000");

        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName,
                new StockTickSerializationSchema(),
                inputProperties));
    }

    private static StreamingFileSink<StockTick> createSink(String sinkPath) {

        return StreamingFileSink
                .forRowFormat(new Path(sinkPath), new DinkySerializer())
                .withBucketAssigner(new IsinBucketAssigner())
                .withRollingPolicy(
                        DefaultRollingPolicy.create()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();
    }

    private static StreamingFileSink<StockTick> createParquetSink(String sinkPath) {

        var writerBuilder = ParquetAvroWriters.forSpecificRecord(StockTick.class);

        return StreamingFileSink
                .forBulkFormat(new Path(sinkPath), writerBuilder)
                .withBucketAssigner(new IsinBucketAssigner())
                .withRollingPolicy(new CustomRollingPolicy())
                .build();
    }


    public static void main(String[] args) throws Exception {

        String sinkPath = "<S3 Bucket in here>>";

        if(args.length > 0){
            sinkPath = args[0];
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(180000L, CheckpointingMode.EXACTLY_ONCE);
        DataStream<StockTick> input = createSource(env);
        input.keyBy(t -> t.getIsin())
                .addSink(createParquetSink(sinkPath))
                .name("S3 Sink");
        env.execute("Flink S3 Streaming Sink Job");
    }

    private static class DinkySerializer implements Encoder<StockTick> {
        @Override
        public void encode(StockTick stockTick, OutputStream outputStream) throws IOException {
            var record = String.format("%s,%f,%t\n", stockTick.getIsin(), stockTick.getBid(), stockTick.getTimeStamp());
            outputStream.write(record.getBytes(StandardCharsets.UTF_8));
        }
    }
}