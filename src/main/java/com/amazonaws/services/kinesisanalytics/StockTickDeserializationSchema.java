package com.amazonaws.services.kinesisanalytics;

import com.google.gson.*;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;

public class StockTickDeserializationSchema implements DeserializationSchema<StockTick> {
    private static final Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .registerTypeAdapter(Instant.class, (JsonDeserializer<Instant>) (json, typeOfT, context) -> Instant.parse(json.getAsString()))
            .create();

    @Override
    public StockTick deserialize(byte[] bytes) throws IOException {
        //parse the event payload and remove the type attribute
        JsonReader jsonReader =  new JsonReader(new InputStreamReader(new ByteArrayInputStream(bytes)));
        JsonElement jsonElement = Streams.parse(jsonReader);

        return gson.fromJson(jsonElement, StockTick.class);
    }

    @Override
    public boolean isEndOfStream(StockTick stockTick) {
        return false;
    }

    @Override
    public TypeInformation<StockTick> getProducedType() {
        return null;
    }
}
