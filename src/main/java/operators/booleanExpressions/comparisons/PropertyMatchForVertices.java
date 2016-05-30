package operators.booleanExpressions.comparisons;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import operators.datastructures.VertexExtended;

import org.apache.flink.api.common.functions.FilterFunction;

@SuppressWarnings("serial")
public class PropertyMatchForVertices implements FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>{

	private HashMap<String, String> properties;
	
	public PropertyMatchForVertices(HashMap<String, String> properties) {
		this.properties = properties;
	}
	@Override
	public boolean filter(
			VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex)
			throws Exception {
		for(Map.Entry<String, String> propInQuery : this.properties.entrySet()) {
			//If the vertex does not contain the specific key
			if(vertex.getProps().get(propInQuery.getKey()) == null || 
					//If the key is contained, check if the value is consistent or not
				!vertex.getProps().get(propInQuery.getKey()).equals(propInQuery.getValue())) {
				return false;
			}
		}
		return true;
	}
}

