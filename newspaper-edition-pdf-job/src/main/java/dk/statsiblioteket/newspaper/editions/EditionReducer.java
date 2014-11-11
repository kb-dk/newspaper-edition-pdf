package dk.statsiblioteket.newspaper.editions;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.pdfbox.exceptions.COSVisitorException;
import org.apache.pdfbox.util.PDFMergerUtility;

import java.io.File;
import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

public class EditionReducer extends Reducer<Text, Text, Text, Text> {

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
        super.reduce(key, values, context);

        SortedSet<Text> sortedSet = new TreeSet<Text>();
        for (Text value : values) {
            sortedSet.add(value);
        }

        PDFMergerUtility merger = new PDFMergerUtility();
        for (Text text : sortedSet) {
            merger.addSource(new File(text.toString()));
        }
        final String destinationFileName = key.toString() + ".pdf";
        merger.setDestinationFileName(destinationFileName);
        try {
            merger.mergeDocuments();
        } catch (COSVisitorException e) {
            throw new IOException(e);
        }
        context.write(key,new Text(destinationFileName));
    }


}
