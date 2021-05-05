package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.data.StockTick;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.specific.SpecificData;
import org.apache.flink.api.common.functions.MapFunction;
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
    private static final String s3SinkPath = "s3a://ca-garbage/data";
    // private static final String s3SinkPath = "file:///Users/mkaluz/ca-garbage/data";

    private static final Logger LOG = LoggerFactory.getLogger(S3StreamingSinkJob.class);

    private static DataStream<StockTick> createSource(StreamExecutionEnvironment env) {

        LOG.debug("Creating source...");

        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION,
                "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName,
                new StockTickDeserializationSchema(),
                inputProperties));
    }

    private static StreamingFileSink<StockTick> createSink() {

        return StreamingFileSink
                .forRowFormat(new Path(s3SinkPath), new DinkySerializer())
                .withBucketAssigner(new IsinBucketAssigner())
                .withRollingPolicy(
                        DefaultRollingPolicy.create()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();
    }

    private static StreamingFileSink<StockTick> createParquetSink() {

        var writerBuilder = ParquetAvroWriters.forSpecificRecord(StockTick.class);

        return StreamingFileSink
                .forBulkFormat(new Path(s3SinkPath), writerBuilder)
                .withBucketAssigner(new IsinBucketAssigner())
                //.withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withRollingPolicy(new CustomRollingPolicy())
                .build();
    }


    public static void main(String[] args) throws Exception {

        SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampConversion());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(60000L, CheckpointingMode.EXACTLY_ONCE);

        DataStream<StockTick> input = createSource(env);

        input.addSink(createParquetSink());

        env.execute("Flink S3 Streaming Sink Job");
    }

    public static final class Tokenizer
            implements MapFunction<String, StockTick> {

        private static final ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JodaModule());

        @Override
        public StockTick map(String s) throws Exception {

            try{
                var stockTick = mapper.readValue(s, StockTick.class);
                return stockTick;
            }
            catch(Throwable e) {
                throw e;
            }
        }
    }

    private static class DinkySerializer implements Encoder<StockTick> {
        @Override
        public void encode(StockTick stockTick, OutputStream outputStream) throws IOException {
            var record = String.format("%s,%f,%t\n", stockTick.getIsin(), stockTick.getBid(), stockTick.getTimeStamp());
            outputStream.write(record.getBytes(StandardCharsets.UTF_8));
        }
    }
}