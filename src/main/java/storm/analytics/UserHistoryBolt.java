package storm.analytics;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class UserHistoryBolt extends BaseRichBolt{
	public static class UserHistory {
		int userId;
		HashSet<String> itemsNavigated;

		public UserHistory(int userId){
			this.userId = userId;
			this.itemsNavigated = new HashSet<String>();
		}
		
		public boolean itemWasPreviouslyNavigated(String itemId) {
			return itemsNavigated.contains(itemId);
		}
		
		public void addNavigatedItem(String itemId) {
			itemsNavigated.add(itemId);
		}

		public Set<String> getItemsNavigated() {
			return Collections.unmodifiableSet(itemsNavigated);
		}
	}

	private static final long serialVersionUID = 1L;
	
	HashMap<Integer, UserHistory> usersHistory;
	OutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.usersHistory = new HashMap<Integer, UserHistory>();
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		int userId = input.getInteger(1);
		String itemId = input.getString(2);
		String categId = input.getString(3);
		UserHistory uh = getUserHistory(userId);

		// If the user previously navigated this item -> ignore it
		if(uh.itemWasPreviouslyNavigated(itemId)) {
			return ;
		}
		else {
			// Otherwise update related items
			for (String otherItem : uh.getItemsNavigated()) {
				// My item doesn't count!
				if(itemId.equals(otherItem)) { 
					break ;
				}
				else {
					// Increment the category counter for the related item
					collector.emit(new Values(itemId, categId));
				}
			}
		}
	}

	private UserHistory getUserHistory(int userId) {
		UserHistory uh = usersHistory.get(userId);
		if(uh == null) {
			uh = new UserHistory(userId);
			usersHistory.put(userId, uh);
		}
		return uh;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("itemId", "categId"));
	}
}
