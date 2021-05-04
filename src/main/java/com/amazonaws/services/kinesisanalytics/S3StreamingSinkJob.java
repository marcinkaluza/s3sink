package com.amazonaws.services.kinesisanalytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class S3StreamingSinkJob {
    private static final String region = "eu-west-1";
    private static final String inputStreamName = "stockprices";
    private static final String s3SinkPath = "s3a://ca-garbage/data";
    //private static final String s3SinkPath = "file:///Users/mkaluz/ca-garbage/data";

    private static final Logger LOG = LoggerFactory.getLogger(S3StreamingSinkJob.class);

    private static DataStream<String> createSource(StreamExecutionEnvironment env) {

        LOG.debug("Creating source...");

        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION,
                "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName,
                new SimpleStringSchema(),
                inputProperties));
    }

    private static StreamingFileSink<ParquetStockTick> createSink() {

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

    private static StreamingFileSink<ParquetStockTick> createParquetSink() {

        var writerBuilder = ParquetAvroWriters.forReflectRecord(ParquetStockTick.class);

        return StreamingFileSink
                .forBulkFormat(new Path(s3SinkPath), writerBuilder)
                .withBucketAssigner(new IsinBucketAssigner())
                //.withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withRollingPolicy(new CustomRollingPolicy())
                .build();
    }


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointStorage("hdfs:///checkpoints-data/");

        DataStream<String> input = createSource(env);

        input.map(new Tokenizer())
                //.keyBy(new KeySelector())
                .map(new StockTickConverter())
                .addSink(createParquetSink());

        env.execute("Flink S3 Streaming Sink Job");
    }

    public static final class Tokenizer
            implements MapFunction<String, StockTick> {

        private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

        @Override
        public StockTick map(String s) throws Exception {

            var stockTick = mapper.readValue(s, StockTick.class);
            return stockTick;
        }
    }

    public static final class KeySelector implements org.apache.flink.api.java.functions.KeySelector<StockTick, String> {

        @Override
        public String getKey(StockTick stockTick) throws Exception {
            return stockTick.getIsin();
        }
    }


    private static class DinkySerializer implements Encoder<ParquetStockTick> {
        @Override
        public void encode(ParquetStockTick stockTick, OutputStream outputStream) throws IOException {
            var record = String.format("%s,%f,%s\n", stockTick.getIsin(), stockTick.getBid(), stockTick.getTimeStamp());
            outputStream.write(record.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static class  StockTickConverter implements MapFunction<StockTick, ParquetStockTick> {

        @Override
        public ParquetStockTick map(StockTick stockTick) throws Exception {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            var time = dateFormat.format(stockTick.getTimeStamp());

            return new ParquetStockTick(stockTick.getIsin(),
                    time,
                    stockTick.getAsk(),
                    stockTick.getBid());
        }
    }
}