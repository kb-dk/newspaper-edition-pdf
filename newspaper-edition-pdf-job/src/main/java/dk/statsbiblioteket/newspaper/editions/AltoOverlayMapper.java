package dk.statsbiblioteket.newspaper.editions;

import dk.statsbiblioteket.doms.central.connectors.BackendInvalidCredsException;
import dk.statsbiblioteket.doms.central.connectors.BackendMethodFailedException;
import dk.statsbiblioteket.doms.central.connectors.EnhancedFedora;
import dk.statsbiblioteket.doms.central.connectors.EnhancedFedoraImpl;
import dk.statsbiblioteket.doms.central.connectors.fedora.pidGenerator.PIDGeneratorException;
import dk.statsbiblioteket.doms.webservices.authentication.Credentials;
import dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.edit.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.font.PDType1Font;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;


public class AltoOverlayMapper extends DomsOverlayMapper {

    /**
     * This Mapper retrieves the alto from doms and overlay it on the single page pdf
     * @param key the edition
     * @param value the filepath
     * @param context the context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);



    }
}
