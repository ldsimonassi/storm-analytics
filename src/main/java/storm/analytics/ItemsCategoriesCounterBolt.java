package storm.analytics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ItemsCategoriesCounterBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	
	public class ItemCounter {
		// Category, number of visits
		HashMap<String, Integer> categories;
		boolean dirty = false;
		String itemId;
		
		public ItemCounter(String itemId) {
			this.itemId = itemId;
			this.categories = new HashMap<String, Integer>();
			try {
				load();
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void isDirty() {
			
		}

		public synchronized int count(String categId) {
			Integer i = categories.get(categId);
			if(i == null) 
				i = new Integer(0);
			i++;
			categories.put(categId, i);
			return i;
		}

		private void load() throws NumberFormatException, IOException {
			dirty = false;
			BufferedReader reader;
			try {
				reader = new BufferedReader(new FileReader(directory+"/"+itemId+".stats"));
			} catch (FileNotFoundException e) {
				return ;
			}
			String line;
			while((line = reader.readLine())!=null) {
				StringTokenizer strTok = new StringTokenizer(",");
				String categId = strTok.nextToken();
				int cant = Integer.valueOf(strTok.nextToken());
			}
		}

		public synchronized void save() {
			// TODO Implement file save.
			dirty = false;
		}
	}

	
	ConcurrentHashMap<String, ItemCounter> items;
	String directory;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.directory = (String)stormConf.get("item-categs-spool-dir");
		startDownloaderThread();
	}

	// Start a thread in charge of downloading metrics to files.
	private void startDownloaderThread() {
		
	}

	@Override
	public void execute(Tuple input) {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("itemId", "categId", "visits"));
	}
}
