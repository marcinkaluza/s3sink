package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.data.StockTick;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.specific.SpecificData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StockTickSerializationSchema implements SerializationSchema<StockTick>, DeserializationSchema<StockTick> {

    private final org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper mapper = new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(StockTickSerializationSchema.class);

    static {
        SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampConversion());
    }

    @Override
    public byte[] serialize(StockTick StockTick) {
        return toJson(StockTick).getBytes();
    }

    @Override
    public StockTick deserialize(byte[] bytes) {
        try {
            org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode node = this.mapper.readValue(bytes, org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode.class);

            return StockTick
                    .newBuilder()
                    .setIsin(node.get("isin").asText())
                    .setTimeStamp(new DateTime(node.get("timeStamp").asText()))
                    .setAsk(node.get("ask").asDouble())
                    .setBid(node.get("bid").asDouble())
                    .build();

        } catch (Exception e) {
            LOG.warn("Failed to serialize StockTick: {}", new String(bytes), e);

            return null;
        }
    }

    @Override
    public boolean isEndOfStream(StockTick StockTick) {
        return false;
    }

    @Override
    public TypeInformation<StockTick> getProducedType() {
        return new AvroTypeInfo<>(StockTick.class);
    }


    public static String toJson(StockTick StockTick) {
        StringBuilder builder = new StringBuilder();

        builder.append("{");
        addTextField(builder, StockTick, "isin");
        builder.append(", ");
        addField(builder, "timeStamp", StockTick.getTimeStamp().getMillis());
        builder.append(", ");
        addField(builder, StockTick, "ask");
        builder.append(", ");
        addField(builder, StockTick, "bid");
        builder.append("}");

        return builder.toString();
    }

    private static void addField(StringBuilder builder, StockTick StockTick, String fieldName) {
        addField(builder, fieldName, StockTick.get(fieldName));
    }

    private static void addField(StringBuilder builder, String fieldName, Object value) {
        builder.append("\"");
        builder.append(fieldName);
        builder.append("\"");

        builder.append(": ");
        builder.append(value);
    }

    private static void addTextField(StringBuilder builder, StockTick StockTick, String fieldName) {
        builder.append("\"");
        builder.append(fieldName);
        builder.append("\"");

        builder.append(": ");
        builder.append("\"");
        builder.append(StockTick.get(fieldName));
        builder.append("\"");
    }
}
