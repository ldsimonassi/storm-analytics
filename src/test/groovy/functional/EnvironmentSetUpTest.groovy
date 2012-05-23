package functional
import org.junit.Test;

class EnvironmentSetUpTest extends AbstractSearchIntegrationTest {
	@Test
	public void itemsApiExists(){
		addItem(1, "new air conditioner with led indicator", 1500, "AIRCONDITION")
		def resp = readItem(1)

		assertEquals(resp.id, 1)
		assertEquals(resp.title, "new air conditioner with led indicator")
		assertEquals(resp.price, 1500)
	}
}
