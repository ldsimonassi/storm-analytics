package storm.analytics;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;


public class UsersNavigationSpout extends AbstractClientSpout {
	private static final long serialVersionUID = 1L;
	String host;
	int port;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.host = (String)conf.get("navigation_host");
		this.port = Integer.valueOf((String)conf.get("navigation_port"));
		super.open(conf, context, collector);
	}
	
	@Override
	protected String getPullHost() {
		return host+":"+port;
	}

	@Override
	protected int getMaxPull() {
		return 15;
	}
}
