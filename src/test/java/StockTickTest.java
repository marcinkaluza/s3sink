import com.amazonaws.services.kinesisanalytics.data.StockTick;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.SneakyThrows;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StockTickTest {

    @Test
    @SneakyThrows
    public void deserializeJson(){
        var json = "{\n" +
                "  \"isin\": \"GB0000100767\",\n" +
                "  \"timeStamp\": \"2021-01-04T08:05:12Z\",\n" +
                "  \"bid\" : 12.45,\n" +
                "  \"ask\" : 12.55\n" +
                "}";

        var expectedTimestamp =  new DateTime(2021, 1, 4, 8, 5, 12, 0);

        var objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        var tick = objectMapper.readValue(json, StockTick.class);


        //assertEquals(expectedTimestamp, tick.getTimeStamp());
        assertEquals("GB0000100767", tick.getIsin() );
        assertEquals(12.45,tick.getBid(),  1E-08);
        assertEquals(12.55, tick.getAsk(), 1E-08);
    }


}
