package dk.statsbiblioteket.newspaper.editions.jp2000;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.awt.image.BufferedImage;
import java.net.URL;

import javax.imageio.ImageIO;

import org.junit.Assert;
import org.junit.Test;

public class JP2000ReadTest {
	@Test
	public void canReadSampleJP2000Image() throws Exception {
		
		String resourceName = "/berlingsketidende-1749-01-03-01-0005A-presentation.jp2";

		URL r = getClass().getResource(resourceName);
		Assert.assertNotNull("cannot locate resource " + resourceName, r);
		
		BufferedImage image = ImageIO.read(r);

		assertNotNull("image==null", image);
		assertEquals(1733, image.getWidth());
		assertEquals(2635, image.getHeight());
		assertEquals(8, image.getColorModel().getPixelSize());
	}
}
