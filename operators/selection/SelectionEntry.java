package operators.selection;

import java.util.Map.Entry;


public class SelectionEntry<K> implements Entry<K, String> {
	String value;
	K key;
	
	@Override
	public String setValue(String value) {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public String getValue() {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public K getKey() {
		// TODO Auto-generated method stub
		return key;
	}

	public SelectionEntry(K key, String value) {
		super();
		this.value = value;
		this.key = key;
	}

}