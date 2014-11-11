package dk.statsiblioteket.newspaper.editions;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UtilsTest {

    @Test
    public void testMap() throws Exception {
        Assert.assertEquals(new Text("1795-06-15-02"),
                                   Utils.getEdition(new Text("B400022028241-RT1/400022028241-1/1795-06-15-02/adresseavisen1759-1795-06-15-02-0005A.jp2")));
    }


}