import com.amazonaws.services.kinesisanalytics.IsinBucketAssigner;
import com.amazonaws.services.kinesisanalytics.data.StockTick;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertEquals;

public class IsinBucketAssignerTest {

    @Test
    public void getBucketId(){
        var bucketAssigner = new IsinBucketAssigner();
        var tick = new StockTick();

        tick.setIsin("MSFT");
        tick.setTimeStamp(Instant.parse("2021-02-03T08:45:13Z"));

        var bucket = bucketAssigner.getBucketId(tick, null);

        assertEquals("MSFT/2021-02-03", bucket);
    }


}
