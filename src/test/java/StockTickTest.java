import com.amazonaws.services.kinesisanalytics.StockTickSerializationSchema;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class StockTickTest {

    @Test
    public void deserializeJson() throws IOException {
        var json = "{\n" +
                "  \"RIC\": \"GB0000100767\",\n" +
                "  \"Date_Time\": \"2021-01-04T08:05:12Z\",\n" +
                "  \"Bid_Price\" : 12.45,\n" +
                "  \"Ask_Price\" : 12.55\n" +
                "}";

        var expectedTimestamp =  new DateTime(2021, 1, 4, 8, 5, 12, 0);

        var schema = new StockTickSerializationSchema();
        var tick = schema.deserialize(json.getBytes(StandardCharsets.UTF_8));


        //assertEquals(expectedTimestamp, tick.getTimeStamp());
        assertEquals("GB0000100767", tick.getRic() );
        assertEquals(12.45,tick.getBidPrice(),  1E-08);
        assertEquals(12.55, tick.getAskPrice(), 1E-08);
    }
}
