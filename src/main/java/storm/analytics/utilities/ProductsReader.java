package storm.analytics.utilities;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import redis.clients.jedis.Jedis;


public class ProductsReader {
	Logger log;
	
	String redisHost;
	int redisPort;
	Jedis jedis;
	
	public ProductsReader(String redisHost, int redisPort) {
	    this.redisHost = redisHost;
	    this.redisPort = redisPort;
	    reconnect();
	}
	
	private void reconnect() {
        jedis = new Jedis(redisHost, redisPort);
    }
	
	public Product readItem(String id) throws Exception{
		String content= jedis.get(id);
		if(content == null  || ("nil".equals(content)))
			return null;
        Object obj=JSONValue.parse(content);
        JSONObject item=(JSONObject)obj;
        Product i= new Product((Long)item.get("id"), (String)item.get("title"), (Long)item.get("price"), (String)item.get("category"));
        return i;
    }

}
