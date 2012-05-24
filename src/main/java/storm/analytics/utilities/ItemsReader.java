package storm.analytics.utilities;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.SingleClientConnManager;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;


public class ItemsReader {
	Logger log;
    String itemsApiHost;
    HttpClient httpclient;
    HttpGet httpget;
	
	public ItemsReader(String itemsApiHost) {
	    this.itemsApiHost = itemsApiHost;
	    reconnect();
	}
	
	private void reconnect() {
        httpclient = new DefaultHttpClient(new SingleClientConnManager());
    }
	
	public Item readItem(String id) throws Exception{
        HttpResponse response;
        BufferedReader reader= null;
        String url= "http://"+itemsApiHost+"/"+id+".json";
        log.debug("Reading item data:["+url+"]");
        httpget = new HttpGet(url);
        try {
            response = httpclient.execute(httpget);

            if(response.getStatusLine().getStatusCode()==200) {
                HttpEntity entity = response.getEntity();
                entity.getContent();
                reader= new BufferedReader(new InputStreamReader(entity.getContent()));
                Object obj=JSONValue.parse(reader);
                JSONObject item=(JSONObject)obj;
                Item i= new Item((Long)item.get("id"), (String)item.get("title"), (Long)item.get("price"), (String)item.get("category"));
                return i;
            } else if (response.getStatusLine().getStatusCode() == 404) {
                response.getEntity().getContent().close();
                return null;
            } else
                throw new Exception(response.getStatusLine().getStatusCode()+" is not a valid HTTP code for this response");
        } catch (Exception e) {
            log.error("Error reading item "+id, e);
            reconnect();
            throw new Exception("Error reading item ["+id+"]", e);
        } finally {
            if(reader!=null){
                try {
                    reader.close();
                } catch (Exception e) {
                    log.error(e);
                }
            }
        }
    }

}
