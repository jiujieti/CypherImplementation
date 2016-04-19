package ldbc.map;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple4;

@SuppressWarnings("serial")
public class PlaceMap implements CrossFunction<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>, 
		Tuple4<Long, String, String, String>, Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> {

	private String[] placeItems;
	private long newId = 1;
	public PlaceMap(String[] placeItems) { this.placeItems = placeItems; }

	@Override
	public Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> cross(
			Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> maxId,
			Tuple4<Long, String, String, String> place) throws Exception {
		
		Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> placeWithOriginId = new Tuple4<>();

		//set vertex id
		placeWithOriginId.f0 = newId + maxId.f0;
		this.newId ++;

		//set vertex labels
		HashSet<String> labels = new HashSet<>();
		labels.add(placeItems[0]);
		placeWithOriginId.f1 = labels;

		//set vertex properties
		HashMap<String, String> properties = new HashMap<>();
		properties.put(placeItems[1], place.f0.toString());
		properties.put(placeItems[2], place.f1.toString());
		properties.put(placeItems[3], place.f2.toString());
		properties.put(placeItems[4], place.f3.toString());
		placeWithOriginId.f2 = properties;
		
		placeWithOriginId.f3 = place.f0;

		return placeWithOriginId;
	}
}
