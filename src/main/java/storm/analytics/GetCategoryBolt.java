package storm.analytics;

import java.util.Map;

import storm.analytics.utilities.Item;
import storm.analytics.utilities.ItemsReader;
import storm.analytics.utilities.NavigationEntry;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.esotericsoftware.minlog.Log;

public class GetCategoryBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	private ItemsReader reader;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		String host = stormConf.get("items-api-host").toString();
		this.reader = new ItemsReader(host); 
		super.prepare(stormConf, context);
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		NavigationEntry entry = (NavigationEntry)input.getValue(1);
		if("ITEM".equals(entry.getPageType())){
			try {
				String itemId = (String)entry.getOtherData().get("itemId");

				// Call the items API to get item information
				Item itm = reader.readItem(itemId);
				String categ = itm.getCategory();

				collector.emit(new Values(entry.getUserId(), itemId, categ));

			} catch (Exception ex) {
				Log.error("Error processing ITEM tuple", ex);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("userId","itemId", "categId"));
	}
}
