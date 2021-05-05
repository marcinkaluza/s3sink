package com.amazonaws.services.kinesisanalytics;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimestampDeserializer extends StdDeserializer<Long> {

    private SimpleDateFormat formatter =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    public TimestampDeserializer() {
        this(null);
    }

    public TimestampDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Long deserialize(JsonParser jsonparser, DeserializationContext context)
            throws IOException, JsonProcessingException {
        String token = jsonparser.getText();
        try {
            var date = formatter.parse(token);
            var millis = date.getTime();
            return millis;
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
