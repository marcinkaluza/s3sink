package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.data.StockTick;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.joda.time.DateTime;

import java.io.IOException;

public class StockTickDeserializationSchema implements DeserializationSchema<StockTick> {


    @Override
    public StockTick deserialize(byte[] bytes) throws IOException {
        var mapper = new ObjectMapper();
        ObjectNode node = mapper.readValue(bytes, ObjectNode.class);

        return StockTick
                .newBuilder()
                .setIsin(node.get("isin").asText())
                .setTimeStamp(new DateTime(node.get("timeStamp").asText()))
                .setBid(node.get("bid").asDouble())
                .setAsk(node.get("ask").asDouble())
                .build();
    }

    @Override
    public boolean isEndOfStream(StockTick stockTick) {
        return false;
    }

    @Override
    public TypeInformation<StockTick> getProducedType() {
        return TypeInformation.of(StockTick.class);
    }
}
