package operators.booleanExpressions.comparisons;

import java.util.HashMap;
import java.util.HashSet;

import operators.datastructures.VertexExtended;

import org.apache.flink.api.common.functions.FilterFunction;

@SuppressWarnings("serial")
public class PropertyFilterForVertices implements FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>{
	
	private String propertyKey;
	private String op;
	private String propertyValue;
	private double propertyValueDouble; 
	
	public PropertyFilterForVertices(String propertyKey, String op, String propertyValue) {
		this.propertyKey = propertyKey;
		this.op = op;
		this.propertyValue = propertyValue;
		try {
			this.propertyValueDouble = Double.parseDouble(propertyValue);
		} 
		catch (Exception e) {}
	}
	
	@Override
	public boolean filter(
			VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex)
			throws Exception {
		if(vertex.getProps().get(this.propertyKey) == null) {
			return false;
		}
		double vp = 0;
		try {
			vp = Double.parseDouble(vertex.f2.get(this.propertyKey));
		}
		catch(Exception e) {}
		switch(op) {
			case ">": {
				return vp > propertyValueDouble;	
			}
			case "<": {
				return vp < propertyValueDouble;
			}
			case "=": {
				return vp == propertyValueDouble;
			}
			case ">=": {
				return vp >= propertyValueDouble;
			}
			case "<=": {
				return vp <= propertyValueDouble;
			}
			case "<>": {
				return vp != propertyValueDouble;
			}
			case "eq": {
				return vertex.f2.get(this.propertyKey).equals(propertyValue);
			}
			default: throw new Exception("Bad operator " + op + " !");
		}
	}
}
