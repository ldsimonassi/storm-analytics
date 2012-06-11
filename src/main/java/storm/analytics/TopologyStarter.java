package storm.analytics;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TopologyStarter {
	public static String REDIS_HOST = "localhost";
	public static int REDIS_PORT = 6379;
	public static String WEBSERVER = "http://localhost:3000/news";

	public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        Logger.getRootLogger().removeAllAppenders();

        
        builder.setSpout("read-feed", new UsersNavigationSpout(), 1);
        builder.setBolt("get-categ", new GetCategoryBolt(), 1).shuffleGrouping("read-feed");
        builder.setBolt("user-history", new UserHistoryBolt(), 1).fieldsGrouping("get-categ", new Fields("user"));
        builder.setBolt("product-categ-counter", new ProductCategoriesCounterBolt(), 1).fieldsGrouping("user-history", new Fields("product"));
        builder.setBolt("news-notifier", new NewsNotifierBolt(), 1).shuffleGrouping("product-categ-counter");
        
        Config conf = new Config();
        conf.setDebug(true);

        conf.put("redis-host", REDIS_HOST);
        conf.put("redis-port", REDIS_PORT);
        conf.put("webserver", WEBSERVER);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("analytics", conf, builder.createTopology());
	}
}