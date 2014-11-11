package dk.statsiblioteket.newspaper.editions;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageModsMapper extends DomsOverlayMapper {
    /**
     * This will retrieve the mods data from doms and overlay it on the pdf.
     * This mapper should also move the input file from it's original location to a new location, of the form
     *  "edition"/"pageNR".pdf. This should be the output'ed key. pageNR should be a 3 digit number.
     * @param key this is the edition
     * @param value this is the pdf
     * @param context to context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //use getFedoraClient and getDomsPid to retrieve the correct stuff from doms
        super.map(key, value, context);
    }
}
