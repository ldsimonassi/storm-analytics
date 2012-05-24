package storm.analytics;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TopologyStarter {
	public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("read-feed", new UsersNavigationSpout(), 2);
        builder.setBolt("get-categ", new GetCategoryBolt(), 2).shuffleGrouping("read-feed");
        builder.setBolt("simple-print", new SimplePrintBolt(), 2).shuffleGrouping("get-categ");

        Config conf = new Config();
        conf.setDebug(true);

        conf.put("navigation_host", "localhost");
        conf.put("navigation_port", "8080");
        conf.put("items-api-host", "localhost:8888");
        conf.put("item-categs-spool-dir", "./tmp/");
        
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("analytics", conf, builder.createTopology());
	}
}
