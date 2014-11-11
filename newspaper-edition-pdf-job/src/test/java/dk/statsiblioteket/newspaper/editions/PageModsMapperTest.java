package dk.statsiblioteket.newspaper.editions;

import com.google.common.io.Files;
import dk.statsbiblioteket.doms.central.connectors.EnhancedFedora;
import dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants;
import dk.statsbiblioteket.medieplatform.hadoop.DomsSaverReducer;
import dk.statsbiblioteket.util.Strings;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

public class PageModsMapperTest {

    @Test
    public void testMap() throws Exception {
        MapDriver<Text, Text, Text, Text> mapDriver;

        String testPid = "uuid:testPid";
        String batchID = "B400022028241-RT1";



        String pageMods = Strings.flush(Thread.currentThread()
                                              .getContextClassLoader()
                                              .getResourceAsStream("pageMods.xml"));

        final EnhancedFedora mockFedora = mock(EnhancedFedora.class);
        when(mockFedora.findObjectFromDCIdentifier(anyString())).thenReturn(Arrays.asList(testPid));
        doReturn(pageMods).when(mockFedora)
                            .getXMLDatastreamContents(eq(testPid), eq("MODS"));

        mapDriver = MapDriver.newMapDriver(new PageModsMapper() {
            @Override
            protected EnhancedFedora createFedoraClient(Context context) throws IOException {
                return mockFedora;
            }
        });


        mapDriver.getConfiguration().setIfUnset(ConfigConstants.BATCH_ID, batchID);
        File editionsDirectory = Files.createTempDir();

        mapDriver.getConfiguration().setIfUnset("editions.tmp.directory", editionsDirectory.getAbsolutePath());
        Text key = new Text("B400022028241-RT1_400022028241-1_1795-06-15-02_adresseavisen1759-1795-06-15-02-0005A.jp2");
        Text value = new Text(File.createTempFile("test",".jp2").getAbsolutePath());

        mapDriver.withInput(key, value);
        mapDriver.withOutput(key, new Text(new File(editionsDirectory,"1795-06-15-02/001.pdf").getAbsolutePath()));
        mapDriver.runTest();
    }

    @Test
    public void testXpath() throws Exception {
        Integer pageNR = PageModsMapper.getPageNr(Strings.flush(Thread.currentThread()
                                                                      .getContextClassLoader()
                                                                      .getResourceAsStream("pageMods.xml")));
        assertEquals(pageNR.intValue(),1);
    }
}