package dk.statsiblioteket.newspaper.editions;

import dk.statsbiblioteket.util.Files;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URISyntaxException;

import static org.testng.Assert.*;

public class EditionAsKeyMapperTest {

    @Test
    public void testMap() throws Exception {
        MapDriver<LongWritable, Text, Text, Text> mapDriver;

        EditionAsKeyMapper mapper = new EditionAsKeyMapper();

        mapDriver = MapDriver.newMapDriver(mapper);


        mapDriver.withInput(new LongWritable(5), new Text("B400022028241-RT1/400022028241-1/1795-06-15-02/adresseavisen1759-1795-06-15-02-0005A.jp2"));

        mapDriver.withOutput(new Text("1795-06-15-02"),
                                    new Text("B400022028241-RT1/400022028241-1/1795-06-15-02/adresseavisen1759-1795-06-15-02-0005A.jp2"));
        mapDriver.runTest();
    }


}