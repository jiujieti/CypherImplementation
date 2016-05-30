package operators.booleanExpressions.comparisons;

import java.util.HashMap;
import java.util.Map;

import operators.datastructures.EdgeExtended;

import org.apache.flink.api.common.functions.FilterFunction;

@SuppressWarnings("serial")
public class PropertyMatchForEdges implements FilterFunction<EdgeExtended<Long, Long, String, HashMap<String, String>>>{

	private HashMap<String, String> properties;
	
	public PropertyMatchForEdges(HashMap<String, String> properties) {
		this.properties = properties;
	}
	@Override
	public boolean filter(
			EdgeExtended<Long, Long, String, HashMap<String, String>> edge)
			throws Exception {
		for(Map.Entry<String, String> propInQuery : this.properties.entrySet()) {
			//If the edge does not contain the specific key
			if(edge.getProps().get(propInQuery.getKey()) == null || 
					//If the key is contained, check if the value is consistent or not
				!edge.getProps().get(propInQuery.getKey()).equals(propInQuery.getValue())) {
				return false;
			}
		}
		return true;
	}
}

