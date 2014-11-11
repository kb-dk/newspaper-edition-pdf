package dk.statsiblioteket.newspaper.editions;

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

        String[] commandBits = commandPath.split(" ");
        List<String> commandList = Arrays.asList(commandBits);
        ArrayList<String> result = new ArrayList<String>(commandList);
        //TODO fix for the real interface to kakadu
        result.addAll(Arrays.asList("-i", dataPath, "-o", resultFile.getAbsolutePath()));
        return result.toArray(new String[result.size()]);
    }
}
