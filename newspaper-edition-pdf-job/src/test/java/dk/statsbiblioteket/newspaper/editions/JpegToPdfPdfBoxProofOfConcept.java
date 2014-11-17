package dk.statsbiblioteket.newspaper.editions;

import org.apache.pdfbox.exceptions.COSVisitorException;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.edit.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.graphics.xobject.PDJpeg;
import org.apache.pdfbox.pdmodel.graphics.xobject.PDXObjectImage;
import org.junit.Assert;
import org.testng.annotations.Test;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JpegToPdfPdfBoxProofOfConcept {

    @Test
    public void test() throws Exception {

        final BufferedImage bufferedImage = canReadSampleJP2000Image();

        File outputfile = new File("saved.png");
        ImageIO.write(bufferedImage, "png", outputfile);

        // Create a document and add a page to it
        PDDocument document = new PDDocument();
        PDPage page = new PDPage(new PDRectangle(bufferedImage.getWidth(),bufferedImage.getHeight()));
        document.addPage(page);


        PDJpeg pdfjpg = new PDJpeg(document, bufferedImage,1.0f);

        PDPageContentStream contentStream = new PDPageContentStream(document, page,false,false);
        contentStream.drawImage(pdfjpg,0,0);
        contentStream.close();

        // Save the results and ensure that the document is properly closed:
        document.save("Hello World2.pdf");
        document.close();
    }

    public BufferedImage canReadSampleJP2000Image() throws Exception {

        String resourceName = "/berlingsketidende-1749-01-03-01-0005A-presentation.jp2";

        URL r = getClass().getResource(resourceName);
        Assert.assertNotNull("cannot locate resource " + resourceName, r);

        return ImageIO.read(r);
    }
}


