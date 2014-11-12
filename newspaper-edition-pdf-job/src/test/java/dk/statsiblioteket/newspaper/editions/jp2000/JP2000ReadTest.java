package dk.statsiblioteket.newspaper.editions.jp2000;

import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.junit.Assert;
import org.junit.Test;

public class JP2000ReadTest {

	@Test
	public void canReadSampleJP2000Image() throws Exception {
		String resource = "berlingsketidende-1749-01-03-01-0005A-presentation.jp2";
		Image image = ImageIO.read(ClassLoader.getSystemResource(resource));
		Assert.assertNotNull("image==null", image);
	}
}
