package storm.analytics;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class SimplePrintBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.println("Value:"+input.getValue(0) + " Content:" +input.getValue(1));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
