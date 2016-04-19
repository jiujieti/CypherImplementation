package ldbc.map;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

@SuppressWarnings("serial")
public class TagMap implements CrossFunction<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>, 
	Tuple3<Long, String, String>, Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> {
	
	private String[] tagItems;
	private long newId = 1;
	public TagMap(String[] tagItems) { this.tagItems = tagItems; }

	@Override
	public Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> cross(
			Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> maxId,
			Tuple3<Long, String, String> tag) throws Exception {


		Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> tagWithOriginId = new Tuple4<>();

		//set vertex id
		tagWithOriginId.f0 = newId + maxId.f0;
		this.newId ++;

		//set vertex labels
		HashSet<String> labels = new HashSet<>();
		labels.add(tagItems[0]);
		tagWithOriginId.f1 = labels;

		//set vertex properties
		HashMap<String, String> properties = new HashMap<>();
		properties.put(tagItems[1], tag.f0.toString());
		properties.put(tagItems[2], tag.f1.toString());
		properties.put(tagItems[3], tag.f2.toString());
		tagWithOriginId.f2 = properties;
		
		tagWithOriginId.f3 = tag.f0;
		
		return tagWithOriginId;
	}
}
