package dk.statsiblioteket.newspaper.editions;

import dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class EditionsPdfJob implements Tool {

    private static Logger log = Logger.getLogger(EditionsPdfJob.class);
    private Configuration conf;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new EditionsPdfJob(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        configuration.setIfUnset(ConfigConstants.DOMS_URL, "http://achernar:7880/fedora");
        configuration.setIfUnset(ConfigConstants.DOMS_USERNAME, "fedoraAdmin");
        configuration.setIfUnset(ConfigConstants.DOMS_PASSWORD, "fedoraAdminPass");
        configuration.setIfUnset(Jp2kToPdfMapper.HADOOP_CONVERTER_OUTPUT_EXTENSION_PATH, ".pdf");
/*
        configuration.setIfUnset(Jp2kToPdfMapper.HADOOP_CONVERTER_OUTPUT_PATH, editionsDirectory.getAbsolutePath());
        configuration.setIfUnset(PageModsMapper.EDITIONS_TMP_DIRECTORY, editionsDirectory.getAbsolutePath());
        configuration.setIfUnset(EditionReducer.EDITIONS_DIRECTORY, editionsDirectory.getAbsolutePath());
*/


        Job job = Job.getInstance(configuration);
        job.setJobName("Newspaper " + getClass().getSimpleName() + " " + configuration.get(ConfigConstants.BATCH_ID));
        job.setJarByClass(EditionsPdfJob.class);
        //They all require this param

        //the jp2topdf mapper requires these config params
        ChainMapper.addMapper(job,
                                     Jp2kToPdfMapper.class,
                                     Text.class,
                                     Text.class,
                                     Text.class,
                                     Text.class,
                                     configuration);
        //This one requires no params yet
        ChainMapper.addMapper(job,
                                     AltoOverlayMapper.class,
                                     Text.class,
                                     Text.class,
                                     Text.class,
                                     Text.class,
                                     configuration);



        ChainMapper.addMapper(job, PageModsMapper.class, Text.class, Text.class, Text.class, Text.class, configuration);

        job.setMapperClass(ChainMapper.class);


        job.setReducerClass(EditionReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(NLineInputFormat.class);
        int filesPerMapTask = configuration.getInt(ConfigConstants.FILES_PER_MAP_TASK, 1);
        NLineInputFormat.setNumLinesPerSplit(job, filesPerMapTask);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        log.info(job);
        return result ? 0 : 1;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }
}

