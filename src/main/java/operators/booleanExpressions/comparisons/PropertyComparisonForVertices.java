package operators.booleanExpressions.comparisons;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import operators.datastructures.VertexExtended;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class PropertyComparisonForVertices implements FlatJoinFunction<ArrayList<Long>, VertexExtended<Long, HashSet<String>,
		HashMap<String, String>>, ArrayList<Long>>{
	
	private String propertyKey;
	private String op;
	private double propertyValue;
	
	public PropertyComparisonForVertices(String propertyKey, String op, double propertyValue) {
		this.propertyKey = propertyKey;
		this.op = op;
		this.propertyValue = propertyValue;
	}
	
	@Override
	public void join(
			ArrayList<Long> vertexId,
			VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex,
			Collector<ArrayList<Long>> selectedVertexId) throws Exception {
		if(vertex.getProps().get(this.propertyKey) == null) {
			return;
		}
		else {
			switch(op) {
				case ">": {
					if(Double.parseDouble(vertex.f2.get(this.propertyKey)) > this.propertyValue) selectedVertexId.collect(vertexId);
					else return;	
				}
				case "<": {
					if(Double.parseDouble(vertex.f2.get(this.propertyKey)) < this.propertyValue) selectedVertexId.collect(vertexId);
					else return;	
				}
				case "=": {
					if(Double.parseDouble(vertex.f2.get(this.propertyKey)) == this.propertyValue) selectedVertexId.collect(vertexId);
					else return;	
				}
				case ">=": {
					if(Double.parseDouble(vertex.f2.get(this.propertyKey)) >= this.propertyValue) selectedVertexId.collect(vertexId);
					else return;
				}
				case "<=": {
					if(Double.parseDouble(vertex.f2.get(this.propertyKey)) <= this.propertyValue) selectedVertexId.collect(vertexId);
					else return;	
				}
				case "<>": {
					if(Double.parseDouble(vertex.f2.get(this.propertyKey)) != this.propertyValue) selectedVertexId.collect(vertexId);
					else return;	
				}
				default: return;
			}
		}
	}
}
