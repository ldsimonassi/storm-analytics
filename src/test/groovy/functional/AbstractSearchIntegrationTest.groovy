package functional

import storm.analytics.*;
import backtype.storm.LocalCluster;
import groovyx.net.http.ContentType;
import groovyx.net.http.RESTClient
import org.junit.Before
import org.junit.After
import org.junit.Assert


public abstract class AbstractSearchIntegrationTest extends Assert {
    def itemsApiClient
    def searchEngineApiClient
    def newsFeedApiClient

	// Storm data structures
	def cluster
	def topology
	def conf

	public static topologyStarted = false
	public static sync= new Object()

	@Before
	public void startTopology(){
		synchronized(sync){
			populateItemsApi();
			if(!topologyStarted){
				TopologyStarter.main(null);
				topologyStarted = true;
				Thread.sleep(1000);
			}
		}
	}

	public void populateItemsApi() {
		addItem(1, "Funny Mp3 player", 200, "MP3")
		addItem(2, "Large capacity Mp3 player", 200, "MP3")
		addItem(3, "32Gb mp3 player", 200, "MP3")

		addItem(4, "Mp3 battery", 200, "BATTERIES")
		addItem(5, "Mp3 Battery with Charger", 200, "BATTERIES")

		addItem(6, "LED Tv", 200, "TVS")
		addItem(7, "LED Tv", 200, "TVS")

		addItem(8, "Portable Speakers", 200, "SPEAKERS")
		addItem(9, "PC Speakers", 200, "SPEAKERS")
		addItem(10, "USB Speakers", 200, "SPEAKERS")
	}
	

	@Before
    public void startRestClients() {
        itemsApiClient        = new RESTClient('http://127.0.0.1:8888')
		clearItems()
    }
    
    
    /**
     * Integration testing helpers.
     */
	public void clearItems() {
		def resp= itemsApiClient.delete(path : "/")
		assertEquals(resp.status, 200)
	}

	public void addItem(int id, String title, int price, String category) {
		def document = "/${id}.json"
		def toSend = [:]
		toSend['id'] = id
		toSend['title'] = title
		toSend['price'] = price
		toSend['category'] = category

		println "Posting item [	${document}] [${toSend}]"
        def resp= itemsApiClient.post(path : document,
                                      body: toSend,
                                      requestContentType: ContentType.JSON)
        assertEquals(resp.status, 200)
	}

	public void removeItem(int id) {
		def document = "/${id}.json"
        def resp= itemsApiClient.delete(path : document)
        assertEquals(resp.status, 200)
	}


	public Object readItem(int id) {
		def document = "/${id}.json"
		def resp = itemsApiClient.get(path:document)
		assertEquals(200, resp.status)
		assertEquals("${id}", "${resp.data.id}")

		return resp.data
	}
}
