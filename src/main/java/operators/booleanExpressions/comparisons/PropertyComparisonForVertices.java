package operators.booleanExpressions.comparisons;

import java.util.HashMap;
import java.util.HashSet;

import operators.datastructures.VertexExtended;

import org.apache.flink.api.common.functions.FilterFunction;

@SuppressWarnings("serial")
public class PropertyComparisonForVertices implements FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>{
	private String propertyKey;
	private String op;
	private double propertyValue;
	
	public PropertyComparisonForVertices(String propertyKey, String op, double propertyValue) {
		this.propertyKey = propertyKey;
		this.op = op;
		this.propertyValue = propertyValue;
	}
	@Override
	public boolean filter(VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex) throws Exception {
		if(vertex.getProps().get(this.propertyKey) == null) {
			return false;
		}
		else {
			switch(op) {
				case ">": {
					if(Double.parseDouble(vertex.f2.get(this.propertyKey)) > this.propertyValue) return true;
					else return false;	
				}
				case "<": {
					if(Double.parseDouble(vertex.f2.get(this.propertyKey)) < this.propertyValue) return true;
					else return false;
				}
				case "=": {
					if(Double.parseDouble(vertex.f2.get(this.propertyKey)) == this.propertyValue) return true;
					else return false;
				}
				case ">=": {
					if(Double.parseDouble(vertex.f2.get(this.propertyKey)) >= this.propertyValue) return true;
					else return false;
				}
				case "<=": {
					if(Double.parseDouble(vertex.f2.get(this.propertyKey)) <= this.propertyValue) return true;
					else return false;
				}
				case "<>": {
					if(Double.parseDouble(vertex.f2.get(this.propertyKey)) != this.propertyValue) return true;
					else return false;
				}
				default: return false;
			}
		}
	}
}
