package ldbc.map;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple4;

@SuppressWarnings("serial")
public class OrganisationMap implements CrossFunction<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>, 
		Tuple4<Long, String, String, String>, Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> {
	
	private String[] organisationItems;
	private long newId = 1;
	public OrganisationMap(String[] organisationItems) { this.organisationItems = organisationItems; }

	@Override
	public Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> cross(
			Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> maxId,
			Tuple4<Long, String, String, String> organisation) throws Exception {
		
		Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> organisationWithOriginId = new Tuple4<>();

		//set vertex id
		organisationWithOriginId.f0 = newId + maxId.f0;
		this.newId ++;

		//set vertex labels
		HashSet<String> labels = new HashSet<>();
		labels.add(organisationItems[0]);
		organisationWithOriginId.f1 = labels;

		//set vertex properties
		HashMap<String, String> properties = new HashMap<>();
		properties.put(organisationItems[1], organisation.f0.toString());
		properties.put(organisationItems[2], organisation.f1.toString());
		properties.put(organisationItems[3], organisation.f2.toString());
		properties.put(organisationItems[4], organisation.f3.toString());
		organisationWithOriginId.f2 = properties;
		
		organisationWithOriginId.f3 = organisation.f0;
		
		return organisationWithOriginId;
	}
}
