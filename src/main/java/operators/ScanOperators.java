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

/*
* A scan operator is used to extract all vertex IDs which satisfy certain filtering conditions.
* The filtering conditions could be:
* (1) no conditions
* (2) filtering conditions on labels of vertices
* (3) filtering conditions on properties of vertices
* (4) a combination of conditions related by complex boolean expressions
* */
@SuppressWarnings("serial")
public class ScanOperators {
	private final GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long,
	  String, HashMap<String, String>> graph;
		
	//Get the input graph
	public ScanOperators(GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long,
			  String, HashMap<String, String>> g) {
		this.graph = g;
	}
	
	//Get vertex IDs of a graph
	public DataSet<ArrayList<Long>> 
		getInitialVertices() {
		DataSet<ArrayList<Long>> vertexIds = graph
			.getVertices()
			.map(new InitialVerticesToLists());
		return vertexIds;
	}

	//Extract vertex IDs into ArrayList from vertices
	private static class InitialVerticesToLists implements MapFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>, ArrayList<Long>> {
		
		@Override
		public ArrayList<Long> map(
				VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex) throws Exception {
			ArrayList<Long> row = new ArrayList<>();
			row.add(vertex.f0);
			return row;
		}
	}
	
	//Get edge IDs of a graph
	//not very useful so far
	public DataSet<ArrayList<Long>> getInitialEdges() {
		DataSet<ArrayList<Long>> edgeIds = graph
			.getEdges()
			.map(new InitialEdgesToLists());
		return edgeIds;
	}

	//Extract edge IDs into ArrayList from edges
	//also not very useful so far
	private static class InitialEdgesToLists implements MapFunction<EdgeExtended<Long, Long, String, HashMap<String, String>>, ArrayList<Long>> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public ArrayList<Long> map(EdgeExtended<Long, Long, String, HashMap<String, String>> edge) throws Exception {
			ArrayList<Long> row = new ArrayList<>();
			row.add(edge.f0);
			return row;
		}
	}
	
	//Get vertex ids with label constraints
	public DataSet<ArrayList<Long>> getInitialVerticesByLabels(HashSet<String> labels) {
		DataSet<ArrayList<Long>> vertexIds = graph
				.getVertices()
				.filter(new FilterVerticesByLabel(labels))
				.map(new InitialVerticesToLists());
		return vertexIds;
				
	}

	//Check whether all labels specified in the query are contained
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
	
	//Get vertex IDs with property constraints
	public DataSet<ArrayList<Long>> getInitialVerticesByProperties(HashMap<String, String> properties) {
		DataSet<ArrayList<Long>> vertexIds = graph
				.getVertices()
				.filter(new FilterVerticesByProperties(properties))
				.map(new InitialVerticesToLists());
		return vertexIds;
	}

	//Check whether all properties specified in the query are existing the corresponding values of certain properties are consistent with ones in the query
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

	//Get vertex IDs filtered by a combination of conditions related by complex boolean expressions
	public DataSet<ArrayList<Long>> getInitialVerticesByBooleanExpressions(
			FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> filterVertices){
		
		DataSet<ArrayList<Long>> vertexIds = graph
				.getVertices()
				.filter(filterVertices)
				.map(new InitialVerticesToLists());
		return vertexIds; 
	}
}