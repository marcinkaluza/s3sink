import com.amazonaws.services.kinesisanalytics.IsinBucketAssigner;
import com.amazonaws.services.kinesisanalytics.data.Quote;
import org.joda.time.DateTime;
import org.junit.Test;


import static org.junit.Assert.assertEquals;

public class IsinBucketAssignerTest {

    @Test
    public void getBucketId(){
        var bucketAssigner = new IsinBucketAssigner();
        var tick = new Quote();

        tick.setRic("MSFT");
        tick.setDateTime(DateTime.parse("2021-02-03T08:45:13Z"));

        var bucket = bucketAssigner.getBucketId(tick, null);

        assertEquals("MSFT/2021-02-03", bucket);
    }


}
