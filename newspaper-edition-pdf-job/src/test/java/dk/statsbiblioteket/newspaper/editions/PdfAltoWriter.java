package dk.statsbiblioteket.newspaper.editions;

import dk.statsbiblioteket.util.xml.DOM;
import dk.statsbiblioteket.util.xml.XPathSelector;
import org.apache.pdfbox.exceptions.COSVisitorException;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.edit.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.testng.annotations.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;

public class PdfAltoWriter {

    @Test
    public void test() throws IOException, COSVisitorException {

        Document altoDOM = DOM.streamToDOM(Thread.currentThread()
                                                 .getContextClassLoader()
                                                 .getResourceAsStream("pageAlto.xml"));

        XPathSelector xp = DOM.createXPathSelector("a", "http://www.loc.gov/standards/alto/ns-v2#");
        // Create a document and add a page to it
        PDDocument document = new PDDocument();
        final Integer height = xp.selectInteger(altoDOM, "/a:alto/a:Layout/a:Page/@HEIGHT")/10;
        final Integer width = xp.selectInteger(altoDOM, "/a:alto/a:Layout/a:Page/@WIDTH")/10;
        PDPage page = new PDPage(new PDRectangle(width,height));
        document.addPage(page);


        // Create a new font object selecting one of the PDF base fonts
        PDFont font = PDType1Font.HELVETICA_BOLD;

        // Start a new content stream which will "hold" the to be created content
        PDPageContentStream contentStream = new PDPageContentStream(document, page);

        NodeList lines = xp.selectNodeList(altoDOM, "//a:TextLine");
        for (int j = 0; j < lines.getLength(); j++) {
            Node line = lines.item(j);
            float fontSize = Float.parseFloat(line.getAttributes()
                                                  .getNamedItem("STYLEREFS")
                                                  .getTextContent()
                                                  .replace("TS", ""));
            contentStream.setFont(font, fontSize*1.5f);
            NodeList strings = xp.selectNodeList(line, "a:String");
            for (int i = 0; i < strings.getLength(); i++) {
                Node string = strings.item(i);

                // Define a text content stream using the selected font, moving the cursor and drawing the text "Hello World"
                int hpos = Integer.parseInt(string.getAttributes().getNamedItem("HPOS").getTextContent()) / 10;
                int vpos = height-Integer.parseInt(string.getAttributes().getNamedItem("VPOS").getTextContent()) / 10;
                contentStream.beginText();
                contentStream.moveTextPositionByAmount(hpos, vpos);

                String content = string.getAttributes().getNamedItem("CONTENT").getTextContent();
                final Node nextRealSibling = nextRealSibling(string);
                if (nextRealSibling != null && "HYP".equals(nextRealSibling.getNodeName())){
                    content = content + "-";
                }
                contentStream.drawString(content);
                contentStream.endText();
            }
        }


        // Make sure that the content stream is closed:
        contentStream.close();

        // Save the results and ensure that the document is properly closed:
        document.save("Hello World.pdf");
        document.close();
    }

    private Node nextRealSibling(Node string) {
        if (string.getNextSibling() == null){
            return null;
        }
        if (string.getNextSibling().getNodeType() != Node.TEXT_NODE){
            return string.getNextSibling();
        } else {
            return nextRealSibling(string.getNextSibling());
        }
    }


    @Test
    public void test2() throws IOException, COSVisitorException {

        Document altoDOM = DOM.streamToDOM(Thread.currentThread()
                                                 .getContextClassLoader()
                                                 .getResourceAsStream("pageAlto.xml"));

        XPathSelector xp = DOM.createXPathSelector("a", "http://www.loc.gov/standards/alto/ns-v2#");
        // Create a document and add a page to it
        PDDocument document = new PDDocument();
        final Integer height = xp.selectInteger(altoDOM, "/a:alto/a:Layout/a:Page/@HEIGHT") / 10;
        final Integer width = xp.selectInteger(altoDOM, "/a:alto/a:Layout/a:Page/@WIDTH") / 10;
        PDPage page = new PDPage(new PDRectangle(width, height));
        document.addPage(page);


        // Create a new font object selecting one of the PDF base fonts
        PDFont font = PDType1Font.HELVETICA_BOLD;

        // Start a new content stream which will "hold" the to be created content
        PDPageContentStream contentStream = new PDPageContentStream(document, page);

        NodeList lines = xp.selectNodeList(altoDOM, "//a:TextLine");
        for (int j = 0; j < lines.getLength(); j++) {
            Node line = lines.item(j);
            float fontSize = Float.parseFloat(line.getAttributes()
                                                  .getNamedItem("STYLEREFS")
                                                  .getTextContent()
                                                  .replace("TS", ""));
            contentStream.setFont(font, fontSize * 1.5f);
            int hpos = Integer.parseInt(line.getAttributes().getNamedItem("HPOS").getTextContent()) / 10;
            int vpos = height - Integer.parseInt(line.getAttributes().getNamedItem("VPOS").getTextContent()) / 10;
            contentStream.beginText();
            contentStream.moveTextPositionByAmount(hpos, vpos);

            NodeList strings = xp.selectNodeList(line, "a:String");
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < strings.getLength(); i++) {
                Node string = strings.item(i);
                String text = string.getAttributes().getNamedItem("CONTENT").getTextContent();
                stringBuilder.append(text).append(" ");
            }
            contentStream.drawString(stringBuilder.toString());
            contentStream.endText();
        }


        // Make sure that the content stream is closed:
        contentStream.close();

        // Save the results and ensure that the document is properly closed:
        document.save("Hello World.pdf");
        document.close();
    }
}
