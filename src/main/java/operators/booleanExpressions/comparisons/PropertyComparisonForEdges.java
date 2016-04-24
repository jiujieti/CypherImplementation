package operators.booleanExpressions.comparisons;

import java.util.HashMap;

import operators.datastructures.EdgeExtended;

import org.apache.flink.api.common.functions.FilterFunction;

@SuppressWarnings("serial")
public class PropertyComparisonForEdges implements FilterFunction<EdgeExtended<Long, Long, String, HashMap<String, String>>>{
	private String propertyKey;
	private String op;
	private double propertyValue;
	
	public PropertyComparisonForEdges(String propertyKey, String op, double propertyValue) {
		this.propertyKey = propertyKey;
		this.op = op;
		this.propertyValue = propertyValue;
	}
	@Override
	public boolean filter(EdgeExtended<Long, Long, String, HashMap<String, String>> edge) throws Exception {
		if(edge.getProps().get(this.propertyKey) == null) {
			return false;
		}
		else {
			switch(op) {
				case ">": {
					if(Double.parseDouble(edge.f4.get(this.propertyKey)) > this.propertyValue) return true;
					else return false;	
				}
				case "<": {
					if(Double.parseDouble(edge.f4.get(this.propertyKey)) < this.propertyValue) return true;
					else return false;
				}
				case "=": {
					if(Double.parseDouble(edge.f4.get(this.propertyKey)) == this.propertyValue) return true;
					else return false;
				}
				case ">=": {
					if(Double.parseDouble(edge.f4.get(this.propertyKey)) >= this.propertyValue) return true;
					else return false;
				}
				case "<=": {
					if(Double.parseDouble(edge.f4.get(this.propertyKey)) <= this.propertyValue) return true;
					else return false;
				}
				case "<>": {
					if(Double.parseDouble(edge.f4.get(this.propertyKey)) != this.propertyValue) return true;
					else return false;
				}
				default: return false;
			}
		}
	}
}
