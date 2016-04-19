package ldbc.map;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple4;


@SuppressWarnings("serial")
public class PersonMap implements CrossFunction<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>, 
		Tuple10<Long, String, String, String, String, String, String, String, String, String>, Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> {

	private String[] personItems;
	private long newId = 1;
	public PersonMap(String[] personItems) { this.personItems = personItems; }

	@Override
	public Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> cross(
			Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> maxId,
			Tuple10<Long, String, String, String, String, String, String, String, String, String> person) throws Exception {
		
		Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> personWithOriginId= new Tuple4<>();
		
		//set id
		personWithOriginId.f0 = newId + maxId.f0;
		this.newId ++;

		//set vertex labels
		HashSet<String> labels = new HashSet<>();
		labels.add(personItems[0]);
		personWithOriginId.f1 = labels;

		//set vertex properties
		HashMap<String, String> properties = new HashMap<>();
		properties.put(personItems[1], person.f0.toString());
		properties.put(personItems[2], person.f1.toString());
		properties.put(personItems[3], person.f2.toString());
		properties.put(personItems[4], person.f3.toString());
		properties.put(personItems[5], person.f4.toString());
		properties.put(personItems[6], person.f5.toString());
		properties.put(personItems[7], person.f6.toString());
		properties.put(personItems[8], person.f7.toString());
		properties.put(personItems[9], person.f8.toString());
		properties.put(personItems[10], person.f9.toString());
		
		personWithOriginId.f2 = properties;

		personWithOriginId.f3 = person.f0;

		return personWithOriginId;
	}
}
