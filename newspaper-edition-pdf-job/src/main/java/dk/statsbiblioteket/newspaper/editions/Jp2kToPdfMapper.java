package dk.statsbiblioteket.newspaper.editions;

import dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants;
import dk.statsbiblioteket.medieplatform.hadoop.ConvertMapper;
import dk.statsbiblioteket.medieplatform.hadoop.Utils;
import dk.statsbiblioteket.util.console.ProcessRunner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Jp2kToPdfMapper extends ConvertMapper{

    @Override
    protected String[] makeCommandLine(String dataPath, String commandPath, File resultFile) {
        return new String[]{"convert",dataPath,resultFile.getAbsolutePath()};
    }
}
