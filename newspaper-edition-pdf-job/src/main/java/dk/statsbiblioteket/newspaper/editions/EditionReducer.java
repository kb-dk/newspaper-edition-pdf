package dk.statsbiblioteket.newspaper.editions;

import com.google.common.io.Files;
import dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.pdfbox.exceptions.COSVisitorException;
import org.apache.pdfbox.util.PDFMergerUtility;

import java.io.File;
import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

public class EditionReducer extends Reducer<Text, Text, Text, Text> {


    protected static final String EDITIONS_DIRECTORY = "editions.directory";
    private File editionsDirectory;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String editionsDir = context.getConfiguration().get(EDITIONS_DIRECTORY);
        String batchID = context.getConfiguration().get(ConfigConstants.BATCH_ID);
        editionsDirectory = new File(editionsDir, batchID);
        editionsDirectory.mkdirs();
    }

    /**
     * This Reducer will retrieve the edition.xml from doms, merge the given pages to one pdf and overlay the metadata.
     * @param key an edition
     * @param values the pages. These can be sorted.
     * @param context the context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        SortedSet<String> sortedSet = new TreeSet<String>();
        for (Text value : values) {
            sortedSet.add(value.toString());
        }

        PDFMergerUtility merger = new PDFMergerUtility();
        merger.addSource(Thread.currentThread().getContextClassLoader().getResourceAsStream("PDF-forside.pdf"));
        for (String text : sortedSet) {
            merger.addSource(new File(text));
        }
        final String destinationFileName = new File(editionsDirectory,key.toString() + ".pdf").getAbsolutePath();
        merger.setDestinationFileName(destinationFileName);
        try {
            merger.mergeDocuments();
        } catch (COSVisitorException e) {
            throw new IOException(e);
        }
        for (String file : sortedSet) {
            new File(file).delete();
        }
        context.write(key,new Text(destinationFileName));
    }


}
