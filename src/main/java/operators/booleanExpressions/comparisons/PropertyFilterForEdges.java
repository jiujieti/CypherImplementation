package operators.booleanExpressions.comparisons;

import java.util.HashMap;

import operators.datastructures.EdgeExtended;

import org.apache.flink.api.common.functions.FilterFunction;

@SuppressWarnings("serial")
public class PropertyFilterForEdges implements FilterFunction<EdgeExtended<Long, Long, String, HashMap<String, String>>>{


	private String propertyKey;
	private String op;
	private String propertyValue;
	private double propertyValueDouble;
	
	public PropertyFilterForEdges(String propertyKey, String op, String propertyValue) {
		this.propertyKey = propertyKey;
		this.op = op;
		try {
			this.propertyValueDouble = Double.parseDouble(propertyValue);
		} 
		catch (Exception e) {}
	}
	
	@Override
	public boolean filter(
			EdgeExtended<Long, Long, String, HashMap<String, String>> edge)
			throws Exception {
		if(edge.getProps().get(this.propertyKey) == null) {
			return false;
		}
		double ep = 0;
		try {
			ep = Double.parseDouble(edge.f4.get(this.propertyKey));
		}
		catch(Exception e) {}
		switch(op) {
			case ">": {
				return ep > propertyValueDouble;	
			}
			case "<": {
				return ep < propertyValueDouble;	
			}
			case "=": {
				return ep == propertyValueDouble; 
			}
			case ">=": {
				return ep >= propertyValueDouble; 
			}
			case "<=": {
				return ep <= propertyValueDouble; 
			}
			case "<>": {
				return ep != propertyValueDouble; 
			}
			case "eq": {
				return edge.f4.get(this.propertyKey).equals(propertyValue);
			}
			default: return false;
			}
		}
}
