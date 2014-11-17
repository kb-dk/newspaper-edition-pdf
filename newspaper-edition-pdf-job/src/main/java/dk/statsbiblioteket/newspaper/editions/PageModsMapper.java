package dk.statsbiblioteket.newspaper.editions;

import com.google.common.io.Files;
import dk.statsbiblioteket.doms.central.connectors.BackendInvalidCredsException;
import dk.statsbiblioteket.doms.central.connectors.BackendInvalidResourceException;
import dk.statsbiblioteket.doms.central.connectors.BackendMethodFailedException;
import dk.statsbiblioteket.util.xml.DOM;
import dk.statsbiblioteket.util.xml.XPathSelector;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.w3c.dom.Document;

import java.io.File;
import java.io.IOException;

public class PageModsMapper extends DomsOverlayMapper {

    protected static final String EDITIONS_TMP_DIRECTORY = "editions.tmp.directory";
    private File editionsDirectory;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String editionsDirString = context.getConfiguration().get(EDITIONS_TMP_DIRECTORY);
        if (editionsDirString == null){
            editionsDirectory = Files.createTempDir();
            editionsDirectory.deleteOnExit();
        } else {
            editionsDirectory = new File(editionsDirString);
        }
    }

    /**
     * This will retrieve the mods data from doms and overlay it on the pdf.
     * This mapper should also move the input file from it's original location to a new location, of the form
     *  "edition"/"pageNR".pdf. This should be the output'ed key. pageNR should be a 3 digit number.
     *  The output key will be the edition name
     * @param key this is the formal name
     * @param value this is the page pdf
     * @param context to context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String pid = getDomsPid(key);
            String modsDatastream = getFedora().getXMLDatastreamContents(pid, "MODS");

            Integer pageNr = getPageNr(modsDatastream);
            final File editionDirectory = new File(editionsDirectory, Utils.getEdition(key.toString()));
            FileUtils.forceMkdir(editionDirectory);
            final File destFile = new File(editionDirectory, String.format("%03d.pdf",pageNr));
            FileUtils.moveFile(new File(value.toString()), destFile);
            context.write(Utils.getEdition(key),new Text(destFile.getAbsolutePath()));
        } catch (BackendInvalidCredsException e) {
            throw new IOException(e);
        } catch (BackendMethodFailedException e) {
            throw new IOException(e);
        } catch (BackendInvalidResourceException e) {
            throw new IOException(e);
        }
    }

    protected static Integer getPageNr(String modsDatastream) {
        Document modsDom = DOM.stringToDOM(modsDatastream,true);
        XPathSelector xpath = DOM.createXPathSelector("mods", "http://www.loc.gov/mods/v3");
        return xpath.selectInteger(modsDom, "/mods:mods/mods:part/mods:extent[@unit='pages']/mods:start");
    }

}
