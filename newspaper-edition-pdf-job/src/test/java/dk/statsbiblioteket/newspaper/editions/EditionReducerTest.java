package dk.statsbiblioteket.newspaper.editions;

import com.google.common.io.Files;
import dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;

import static org.testng.Assert.*;

public class EditionReducerTest {

    @Test
    public void testReduce() throws Exception {
        String page1 = convertPage();
        String page2 = convertPage();
        ReduceDriver<Text,Text,Text,Text> driver;

        String batchID = "B400022028241-RT1";

        driver = ReduceDriver.newReduceDriver(new EditionReducer());
        driver.getConfiguration().setIfUnset(ConfigConstants.BATCH_ID, batchID);

        File editionsDirectory = Files.createTempDir();
        editionsDirectory.deleteOnExit();
        driver.getConfiguration()
                 .setIfUnset("editions.directory", editionsDirectory.getAbsolutePath());


        final Text key
                = Utils.getEdition(new Text("B400022028241-RT1/400022028241-1/1795-06-15-02/adresseavisen1759-1795-06-15-02-0005A.jp2"));
        driver.withInput(key,
                                Arrays.asList(new Text(page1),new Text(page2)));
        driver.withOutput(key,
                                 new Text(new
                                 File(new File(editionsDirectory, batchID), key.toString() + ".pdf").getAbsolutePath()));
        //This test only tests the creating of the file, not that the content is right
        driver.runTest();

    }

    public String convertPage() throws Exception {


        Jp2kToPdfMapper mapper = new Jp2kToPdfMapper();
        MapDriver<Text, Text, Text, Text> mapDriver;
        mapDriver = MapDriver.newMapDriver(mapper);

        String batchID = "B400022028241-RT1";


        File editionsDirectory = Files.createTempDir();

        mapDriver.getConfiguration().setIfUnset(Jp2kToPdfMapper.HADOOP_CONVERTER_OUTPUT_EXTENSION_PATH, ".pdf");
        mapDriver.getConfiguration()
                 .setIfUnset(Jp2kToPdfMapper.HADOOP_CONVERTER_OUTPUT_PATH, editionsDirectory.getAbsolutePath());
        mapDriver.getConfiguration().setIfUnset(Jp2kToPdfMapper.HADOOP_CONVERTER_PATH, "convert");

        mapDriver.getConfiguration().setIfUnset(ConfigConstants.BATCH_ID, batchID);

        final String jp2 = "berlingsketidende-1749-01-03-01-0005A-presentation.jp2";
        Text key = new Text(new File(Thread.currentThread()
                                           .getContextClassLoader()
                                           .getResource(jp2)
                                           .toURI()).getAbsolutePath());

        mapDriver.withInput(key, key);
        final String result = new File(new File(editionsDirectory, batchID), jp2 + ".pdf").getAbsolutePath();
        mapDriver.withOutput(key, new Text(result));
        mapDriver.runTest();
        return result;
    }
}