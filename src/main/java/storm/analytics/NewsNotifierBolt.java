package storm.analytics;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class NewsNotifierBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	
	String newsFile;
	FileWriter fw;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.newsFile = (String)stormConf.get("news-file");
		try {
			fw = new FileWriter(newsFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input) {
		String product = input.getString(1);
		String categ = input.getString(2);
		int visits = input.getInteger(3);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// This bolt does not emmit any tuple
	}
}
