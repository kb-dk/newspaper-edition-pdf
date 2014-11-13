package dk.statsiblioteket.newspaper.editions;

import com.google.common.io.Files;
import dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.Assert.*;

public class Jp2kToPdfMapperTest {

    @Test
    public void testMap() throws Exception {


        Jp2kToPdfMapper mapper = new Jp2kToPdfMapper();
        MapDriver<Text, Text, Text, Text> mapDriver;
        mapDriver = MapDriver.newMapDriver(mapper);

        String batchID = "B400022028241-RT1";


        File editionsDirectory = Files.createTempDir();
        editionsDirectory.deleteOnExit();

        mapDriver.getConfiguration().setIfUnset(Jp2kToPdfMapper.HADOOP_CONVERTER_OUTPUT_EXTENSION_PATH, ".pdf");
        mapDriver.getConfiguration().setIfUnset(Jp2kToPdfMapper.HADOOP_CONVERTER_OUTPUT_PATH, editionsDirectory.getAbsolutePath());
        mapDriver.getConfiguration()
                 .setIfUnset(Jp2kToPdfMapper.HADOOP_CONVERTER_PATH, "convert");

        mapDriver.getConfiguration().setIfUnset(ConfigConstants.BATCH_ID, batchID);

        final String jp2 = "berlingsketidende-1749-01-03-01-0005A-presentation.jp2";
        Text key = new Text(new File(Thread.currentThread()
                                           .getContextClassLoader()
                                           .getResource(jp2)
                                           .toURI()).getAbsolutePath());

        mapDriver.withInput(key, key);
        mapDriver.withOutput(key, new Text(new File(new File(editionsDirectory,batchID),jp2+".pdf").getAbsolutePath()));
        mapDriver.runTest();
    }
}