package ldbc.map;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

@SuppressWarnings("serial")
public class TagClassMap implements CrossFunction<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>, 
	Tuple3<Long, String, String>, Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> {
	
	private String[] tagclassItems;
	private long newId = 1;
	public TagClassMap(String[] tagclassItems) { this.tagclassItems = tagclassItems; }

	@Override
	public Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> cross(
			Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> maxId,
			Tuple3<Long, String, String> tagclass) throws Exception {

		Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> tagclassWithOriginId = new Tuple4<>();

		//set vertex id
		tagclassWithOriginId.f0 = newId + maxId.f0;
		this.newId ++;

		//set vertex labels
		HashSet<String> labels = new HashSet<>();
		labels.add(tagclassItems[0]);
		tagclassWithOriginId.f1 = labels;
		
		//set vertex properties
		HashMap<String, String> properties = new HashMap<>();
		properties.put(tagclassItems[1], tagclass.f0.toString());
		properties.put(tagclassItems[2], tagclass.f1.toString());
		properties.put(tagclassItems[3], tagclass.f2.toString());
		tagclassWithOriginId.f2 = properties;
		
		tagclassWithOriginId.f3 = tagclass.f0;
		
		return tagclassWithOriginId;
	}
}
