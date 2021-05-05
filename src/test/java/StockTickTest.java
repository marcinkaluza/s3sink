import com.amazonaws.services.kinesisanalytics.StockTickDeserializationSchema;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class StockTickTest {

    @Test
    public void deserializeJson() throws IOException {
        var json = "{\n" +
                "  \"isin\": \"GB0000100767\",\n" +
                "  \"timeStamp\": \"2021-01-04T08:05:12Z\",\n" +
                "  \"bid\" : 12.45,\n" +
                "  \"ask\" : 12.55\n" +
                "}";

        var expectedTimestamp =  new DateTime(2021, 1, 4, 8, 5, 12, 0);

        var schema = new StockTickDeserializationSchema();
        var tick = schema.deserialize(json.getBytes(StandardCharsets.UTF_8));


        //assertEquals(expectedTimestamp, tick.getTimeStamp());
        assertEquals("GB0000100767", tick.getIsin() );
        assertEquals(12.45,tick.getBid(),  1E-08);
        assertEquals(12.55, tick.getAsk(), 1E-08);
    }


}
