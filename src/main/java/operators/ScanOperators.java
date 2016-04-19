package operators;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;
import operators.datastructures.VertexExtended;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;


@SuppressWarnings("serial")
public class ScanOperators {
	private final GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long,
	  String, HashMap<String, String>> graph;
		
	//get the input graph
	public ScanOperators(GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long,
			  String, HashMap<String, String>> g) {
		this.graph = g;
	}
	
	// get the initial vertex ids of the graph
	public DataSet<ArrayList<Long>> 
		getInitialVertices() {
		DataSet<ArrayList<Long>> vertexIds = graph
			.getVertices()
			.map(new InitialVerticesToLists());
		return vertexIds;
	}
	
	private static class InitialVerticesToLists implements MapFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>, ArrayList<Long>> {
		
		@Override
		public ArrayList<Long> map(
				VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex) throws Exception {
			ArrayList<Long> row = new ArrayList<>();
			row.add(vertex.f0);
			return row;
		}
	}
	
	//get the initial edge ids of the graph
	//not very useful so far
	public DataSet<ArrayList<Long>>
		getInitialEdges() {
		DataSet<ArrayList<Long>> edgeIds = graph
			.getEdges()
			.map(new InitialEdgesToLists());
		return edgeIds;
	}
	
	private static class InitialEdgesToLists implements MapFunction<EdgeExtended<Long, Long, String, HashMap<String, String>>, ArrayList<Long>> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public ArrayList<Long> map(EdgeExtended<Long, Long, String, HashMap<String, String>> edge) throws Exception {
			ArrayList<Long> row = new ArrayList<>();
			row.add(edge.f0);
			return row;
		}
	}
	
	//get the initial vertex ids with label constraints
	public DataSet<ArrayList<Long>> getInitialVerticesByLabels(HashSet<String> labels) {
		DataSet<ArrayList<Long>> vertexIds = graph
				.getVertices()
				.filter(new FilterVerticesByLabel(labels))
				.map(new InitialVerticesToLists());
		return vertexIds;
				
	}
	
	private static class FilterVerticesByLabel implements FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> {
		private HashSet<String> labels;
		
		public FilterVerticesByLabel (HashSet<String> labels) {this.labels = labels;}

		@Override
		public boolean filter(
				VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex)
				throws Exception {
			if(vertex.getLabels().containsAll(this.labels)) 
				return true;
			else return false;
		}	
	}
	
	//get the initial vertex ids with label constraints
	public DataSet<ArrayList<Long>> getInitialVerticesByProperties(HashMap<String, String> properties) {
		DataSet<ArrayList<Long>> vertexIds = graph
				.getVertices()
				.filter(new FilterVerticesByProperties(properties))
				.map(new InitialVerticesToLists());
		return vertexIds;
	}
		
		private static class FilterVerticesByProperties implements FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> {
		    HashMap<String, String> properties;
			
			public FilterVerticesByProperties (HashMap<String, String> properties) {this.properties = properties;}

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
}
	
	

