package storm.analytics;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class UserHistory {
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
