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
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;

public abstract class DomsOverlayMapper extends Mapper<Text, Text, Text, Text> {

    private static Logger log = Logger.getLogger(DomsOverlayMapper.class);

    private String batchID;
    private EnhancedFedora fedora;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        batchID = context.getConfiguration().get(ConfigConstants.BATCH_ID);
        fedora = createFedoraClient(context);
    }

    /**
     * Get the fedora client
     *
     * @param context the hadoop context
     *
     * @return the fedora client
     * @throws java.io.IOException
     */
    @SuppressWarnings("deprecation")//Credentials
    protected EnhancedFedora createFedoraClient(Context context) throws IOException {
        try {
            Configuration conf = context.getConfiguration();

            String username = conf.get(ConfigConstants.DOMS_USERNAME);
            String password = conf.get(ConfigConstants.DOMS_PASSWORD);
            String domsUrl = conf.get(ConfigConstants.DOMS_URL);
            return new EnhancedFedoraImpl(new Credentials(username, password), domsUrl, null, null);
        } catch (JAXBException e) {
            throw new IOException(e);
        } catch (PIDGeneratorException e) {
            throw new IOException(e);
        }
    }

    /**
     * Get the doms pid from the filename
     *
     * @param key the filename
     *
     * @return the doms pid
     */
    protected String getDomsPid(Text key) throws BackendInvalidCredsException, BackendMethodFailedException {
        //TODO cache this

        String filePath = translate(key.toString());
        String path = "path:" + filePath;
        List<String> hits = fedora.findObjectFromDCIdentifier(path);
        if (hits.isEmpty()) {

            throw new RuntimeException("Failed to look up doms object for DC identifier '" + path + "'");
        } else {
            if (hits.size() > 1) {
                log.warn("Found multipe pids for dc identifier '" + path + "', using the first one '" + hits.get(0) + "'");
            }
            return hits.get(0);
        }
    }

    /**
     * Translate the filename back to the original path as stored in doms
     *
     * @param file the filename
     *
     * @return the original path
     */
    protected String translate(String file) {
        return file.substring(file.indexOf(batchID)).replaceAll("_", "/");
    }


    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }

    public String getBatchID() {
        return batchID;
    }

    public EnhancedFedora getFedora() {
        return fedora;
    }
}
