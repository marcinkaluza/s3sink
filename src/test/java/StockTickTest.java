import com.amazonaws.services.kinesisanalytics.StockTick;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.junit.Test;
import static org.junit.Assert.*;

public class StockTickTest {

    @Test
    @SneakyThrows
    public void deserializeJson(){
        var json = "{\n" +
                "  \"isin\": \"GB0000100767\",\n" +
                "  \"timeStamp\": \"2021-01-04T08:01:12Z\",\n" +
                "  \"bid\" : 12.45,\n" +
                "  \"ask\" : 12.55\n" +
                "}";

        var objectMapper = new ObjectMapper();
        var tick = objectMapper.readValue(json, StockTick.class);

        assertEquals("2021-01-04T08:01:12Z", tick.getTimeStampAsString());
        assertEquals("GB0000100767", tick.getIsin() );
        assertEquals(12.45,tick.getBid(),  1E-08);
        assertEquals(12.55, tick.getAsk(), 1E-08);
        assertEquals("GB0000100767/2021-01-04", tick.getBucketKey() );
    }
}
