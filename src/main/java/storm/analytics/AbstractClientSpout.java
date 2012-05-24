package storm.analytics;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;



import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.SingleClientConnManager;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import storm.analytics.utilities.NavigationEntry;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public abstract class AbstractClientSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	public static final int TIMEOUT= 1000;
	transient Logger log;
	transient HttpClient httpclient;
	transient LinkedBlockingQueue<NavigationEntry> pendingRequests;
	transient HttpGet httpget;
	transient Thread t;
	transient boolean isDying = false;
	
	@SuppressWarnings("rawtypes")
	Map conf;
	TopologyContext context;
	SpoutOutputCollector collector;

	protected abstract String getPullHost();
	protected abstract int getMaxPull();

	/**
	 * Prepare the spout to run, means:
	 * 	- Pulling thread startup.
	 *  - Queue creation.
	 *  - Logger creation.
	 *  - HttpClient creation/reconnection.
	 */
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		log = Logger.getLogger(this.getClass());
		this.conf= conf;
		this.context= context;
		this.collector= collector;
		this.pendingRequests = new LinkedBlockingQueue<NavigationEntry>(1000);
		this.isDying = false;
		reconnect();
		t = new Thread("ClientSpout["+this.getClass().getName()+"] pulling:"+"http://"+getPullHost()+"/?max="+getMaxPull()){
			@Override
			public void run() {
				while(true) {
					try {
						if(isDying)
							return ;
						executeGet();
					} catch(Throwable t) {
						log.error("Error in executeGet", t);
						try {Thread.sleep(1000);} catch (InterruptedException e) {}
					}
				}
			}
		};
		t.start();
	}

	private void reconnect() {
		httpclient = new DefaultHttpClient(new SingleClientConnManager()); 
		httpget = new HttpGet("http://"+getPullHost()+"/?max="+getMaxPull()); 
	}

	@Override
	public void close() {
		isDying = true;
		pendingRequests.clear();
		log.info("Finishing client spout ["+this.getClass().getName()+"]");
	}

	@Override
	public void nextTuple() {
		NavigationEntry entry;
		try {
			entry = pendingRequests.poll(1, TimeUnit.SECONDS);
			if(entry == null)
				return;
			
			collector.emit(new Values(entry.getUserId(), entry));
		} catch (InterruptedException e) {
			log.error("Polling interrrupted", e);
		}
	}
	
	public static class Request {
		public String content;
	}
	
	public void executeGet(){
		HttpResponse response;
		BufferedReader reader= null;
		try {
			log.debug("Executing Spout GET HTTP METHOD");
			response = httpclient.execute(httpget);
			log.debug("Executed!");
			HttpEntity entity = response.getEntity();
			reader= new BufferedReader(new InputStreamReader(entity.getContent()));
			JSONArray navigations=(JSONArray)JSONValue.parse(reader);
			if(navigations == null) {
				System.out.println("***** Nothing ****");
			}
			else {
				System.out.println("****** "+navigations.size()+" *********");
				for (Object o : navigations) {
					JSONObject obj= (JSONObject)o;
					JSONObject otherData = (JSONObject)obj.get("otherData");
					
					NavigationEntry entry = new NavigationEntry((String)obj.get("userId"), 
																(String)obj.get("pageType"), 
																otherData);
					boolean inserted = pendingRequests.offer(entry, 10, TimeUnit.SECONDS);
					if(!inserted) 
						log.error("pendingRequests queue is full, discarding request ["+entry+"]");
				}
			}
		} catch (Exception e) {
			log.error("Error in GET Method of client spout", e);
			reconnect();
		} finally {
			if(reader!=null)
				try {
					reader.close();
				} catch (Exception e) {
					log.error("Error closing reader, finally block", e);
				}
		}
	}


	@Override
	public void ack(Object msgId) {
		// Not used in this example, message delivery not warranted
	}

	@Override
	public void fail(Object msgId) {
		// Not used in this example, message delivery not warranted
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("userId", "navigation"));
	}
}
