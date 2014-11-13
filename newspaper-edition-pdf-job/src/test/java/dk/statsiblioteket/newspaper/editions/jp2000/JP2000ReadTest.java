package dk.statsiblioteket.newspaper.editions.jp2000;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.IOException;

import javax.imageio.ImageIO;


public class JP2000ReadTest {

	@Test(enabled = false)
	public void canReadSampleJP2000Image() throws Exception {
		String resource = "berlingsketidende-1749-01-03-01-0005A-presentation.jp2";
		BufferedImage image = ImageIO.read(ClassLoader.getSystemResource(resource));
		Assert.assertNotNull(image,"image==null");
		Assert.assertEquals(1733, image.getWidth());
		Assert.assertEquals(2635, image.getHeight());
		Assert.assertEquals(8, image.getColorModel().getPixelSize());
	}
}
