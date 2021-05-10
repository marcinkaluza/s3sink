package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.data.Quote;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.specific.SpecificData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StockTickSerializationSchema implements SerializationSchema<Quote>, DeserializationSchema<Quote> {

    private final org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper mapper = new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(StockTickSerializationSchema.class);

    static {
        SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampConversion());
    }

    @Override
    public byte[] serialize(Quote Quote) {
        return toJson(Quote).getBytes();
    }

    @Override
    public Quote deserialize(byte[] bytes) {
        try {
            org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode node = this.mapper.readValue(bytes, org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode.class);

            return Quote
                    .newBuilder()
                    .setRic(node.get("RIC").asText())
                    .setDateTime(new DateTime(node.get("Date_Time").asText()))
                    .setAskPrice(node.get("Ask_Price").asDouble())
                    .setBidPrice(node.get("Bid_Price").asDouble())
                    .build();

        } catch (Exception e) {
            LOG.warn("Failed to serialize StockTick: {}", new String(bytes), e);

            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Quote Quote) {
        return false;
    }

    @Override
    public TypeInformation<Quote> getProducedType() {
        return new AvroTypeInfo<>(Quote.class);
    }


    public static String toJson(Quote Quote) {
        StringBuilder builder = new StringBuilder();

        builder.append("{");
        addTextField(builder, Quote, "RIC");
        builder.append(", ");
        addField(builder, "Date_Time", Quote.getDateTime().getMillis());
        builder.append(", ");
        addField(builder, Quote, "Ask_Price");
        builder.append(", ");
        addField(builder, Quote, "Bid_Price");
        builder.append("}");

        return builder.toString();
    }

    private static void addField(StringBuilder builder, Quote Quote, String fieldName) {
        addField(builder, fieldName, Quote.get(fieldName));
    }

    private static void addField(StringBuilder builder, String fieldName, Object value) {
        builder.append("\"");
        builder.append(fieldName);
        builder.append("\"");

        builder.append(": ");
        builder.append(value);
    }

    private static void addTextField(StringBuilder builder, Quote Quote, String fieldName) {
        builder.append("\"");
        builder.append(fieldName);
        builder.append("\"");

        builder.append(": ");
        builder.append("\"");
        builder.append(Quote.get(fieldName));
        builder.append("\"");
    }
}
