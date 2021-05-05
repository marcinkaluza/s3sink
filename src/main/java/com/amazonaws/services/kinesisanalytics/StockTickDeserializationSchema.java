package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.data.StockTick;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class StockTickDeserializationSchema implements DeserializationSchema<StockTick> {
    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public StockTick deserialize(byte[] bytes) throws IOException {
        //parse the event payload and remove the type attribute
        var stockTick = mapper.readValue(bytes, StockTick.class);
        return stockTick;
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
