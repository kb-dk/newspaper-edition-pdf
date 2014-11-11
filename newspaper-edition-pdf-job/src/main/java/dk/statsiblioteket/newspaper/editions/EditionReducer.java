package dk.statsiblioteket.newspaper.editions;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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
    protected void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
