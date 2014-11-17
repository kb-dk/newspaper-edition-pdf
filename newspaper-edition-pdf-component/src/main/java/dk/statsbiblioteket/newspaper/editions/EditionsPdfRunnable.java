package dk.statsbiblioteket.newspaper.editions;

import dk.statsbiblioteket.medieplatform.hadoop.AbstractHadoopRunnableComponent;
import dk.statsbiblioteket.newspaper.editions.EditionsPdfJob;

import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class EditionsPdfRunnable extends AbstractHadoopRunnableComponent {


    private static Logger log = LoggerFactory.getLogger(EditionsPdfRunnable.class);


    /**
     * Constructor matching super. Super requires a properties to be able to initialise the tree iterator, if needed.
     * If you do not need the tree iterator, ignore properties.
     *
     * You can use properties for your own stuff as well
     *
     * @param properties properties
     *
     * @see #getProperties()
     */
    public EditionsPdfRunnable(Properties properties) {
        super(properties);
    }

    @Override
    protected Tool getTool() {
        return new EditionsPdfJob();
    }

    /**
     * This is the event ID that correspond to the work done by this component. It will be added to the list of
     * events a batch have experienced when the work is completed (along with information about success or failure
     */
    @Override
    public String getEventID() {
        return "Editionized";
    }
}
