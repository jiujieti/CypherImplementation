package operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import operators.booleanExpressions.comparisons.PropertyComparisonForVertices;
import operators.datastructures.*;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class UnaryOperators {
	//Input graph
	private GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long,
	  String, HashMap<String, String>> graph;
	
	//Each list contains the vertex IDs and edge IDs of a selected path so far 
	private DataSet<ArrayList<Long>> paths;
	
	//Get the input graph, current columnNumber and the vertex and edges IDs
	public UnaryOperators(GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long,
			  String, HashMap<String, String>> g,
			  DataSet<ArrayList<Long>> paths) {
		this.graph = g;
		this.paths = paths;
	}
	
	//No specific queries on the current vertices
	public DataSet<ArrayList<Long>> selectVertices() {
		return paths;
	}
	
	//Select all vertices by their labels
	public DataSet<ArrayList<Long>> selectVerticesByLabels(int col, HashSet<String> labs){
		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
		DataSet<ArrayList<Long>> selectedResults = paths
			 //Join with the vertices in the input graph then filter these vertices based on labels
			 .join(graph.getVertices())
			 .where(verticesSelector)
			 .equalTo(0)
			 .with(new JoinAndFilterVerticesByLabels(labs));
		
		this.paths = selectedResults;
		return selectedResults;
	}

	private static class JoinAndFilterVerticesByLabels implements
		FlatJoinFunction<ArrayList<Long>, VertexExtended<Long, HashSet<String>,
		HashMap<String, String>>, ArrayList<Long>> {

		private HashSet<String> labels;
		JoinAndFilterVerticesByLabels(HashSet<String> labelSet) { this.labels = labelSet;}
		@Override
		public void join(
				ArrayList<Long> edgesAndVertices,
				VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex,
				Collector<ArrayList<Long>> outEdgesAndVertices)
						throws Exception {
			//Check if the labels mentioned in the query are contained in the vertex label list
			if(vertex.getLabels().containsAll(labels))
				outEdgesAndVertices.collect(edgesAndVertices);
		}	
	}

	
	//select all vertices not including the label
	public DataSet<ArrayList<Long>> selectReverseVerticesByLabels(int col, HashSet<String> labs){
		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
		DataSet<ArrayList<Long>> selectedResults = paths
				 //Join with the vertices in the input graph then filter these vertices based on labels
				 .join(graph.getVertices())
				 .where(verticesSelector)
				 .equalTo(0)
				 .with(new JoinAndFilterReverseVerticesByLabels(labs));
			
			this.paths = selectedResults;
			return selectedResults;
	}
	
	private static class JoinAndFilterReverseVerticesByLabels implements 
		FlatJoinFunction<ArrayList<Long>, VertexExtended<Long, HashSet<String>,
			HashMap<String, String>>, ArrayList<Long>> {

		private HashSet<String> labels;
		JoinAndFilterReverseVerticesByLabels(HashSet<String> labelSet) { this.labels = labelSet;}
	
		@Override
	
		public void join(
				ArrayList<Long> edgesAndVertices,
				VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex,
				Collector<ArrayList<Long>> outEdgesAndVertices)
						throws Exception {
		//Check if the labels mentioned in the query are contained in the vertex label list
			if(!vertex.getLabels().containsAll(labels))
				outEdgesAndVertices.collect(edgesAndVertices);
			}	
		}
	
	//select all vertices by their properties
	public DataSet<ArrayList<Long>> selectVerticesByProperties(int col, HashMap<String, String> props){
		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
		DataSet<ArrayList<Long>> selectedResults = paths
			//Join with the vertices in the input graph then filter these vertices based on properties
			.join(graph.getVertices())
			.where(verticesSelector)
			.equalTo(0)
			.with(new JoinAndFilterVerticesByProperties(props));
		
		this.paths = selectedResults;
		return selectedResults;
	}
	
	private static class JoinAndFilterVerticesByProperties implements
		FlatJoinFunction<ArrayList<Long>, VertexExtended<Long, HashSet<String>,
			HashMap<String, String>>, ArrayList<Long>> {

			private HashMap<String, String> props;
			JoinAndFilterVerticesByProperties(HashMap<String, String> properties) { this.props = properties;}

			@Override
			public void join(
					ArrayList<Long> edgesAndVertices,
					VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex,
					Collector<ArrayList<Long>> outEdgesAndVertices)
							throws Exception {
				//For each property (key-value pair) appeared in query
				for(Map.Entry<String, String> propInQuery : props.entrySet()) {
					//If the vertex does not contain the specific key
					if(vertex.getProps().get(propInQuery.getKey()) == null || 
							//If the key is contained, check if the value is consistent or not
							!vertex.getProps().get(propInQuery.getKey()).equals(propInQuery.getValue())) 
						return;
				}
				outEdgesAndVertices.collect(edgesAndVertices);
			}	
	}
	
	//Select vertices by property comparisons
	public DataSet<ArrayList<Long>> selectVerticesByPropertyComparisons(int col, String propertyKey, String op,
			double propertyValue){
		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
		DataSet<ArrayList<Long>> selectedResults = paths
			//Join with the vertices in the input graph then filter these vertices based on properties
			.join(graph.getVertices())
			.where(verticesSelector)
			.equalTo(0)
			.with(new PropertyComparisonForVertices(propertyKey, op, propertyValue));
		
		this.paths = selectedResults;
		return selectedResults;
	}
	
	
	//select all vertices not including the properties
	public DataSet<ArrayList<Long>> selectReverseVerticesByProperties(int col, HashMap<String, String> props){
		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
		DataSet<ArrayList<Long>> selectedResults = paths
			//Join with the vertices in the input graph then filter these vertices based on properties
			.join(graph.getVertices())
			.where(verticesSelector)
			.equalTo(0)
			.with(new JoinAndFilterReverseVerticesByProperties(props));
		
		this.paths = selectedResults;
		return selectedResults;
	}
	
	private static class JoinAndFilterReverseVerticesByProperties implements
		FlatJoinFunction<ArrayList<Long>, VertexExtended<Long, HashSet<String>,
			HashMap<String, String>>, ArrayList<Long>> {

			private HashMap<String, String> props;
			JoinAndFilterReverseVerticesByProperties(HashMap<String, String> properties) { this.props = properties;}

			@Override
			public void join(
					ArrayList<Long> edgesAndVertices,
					VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex,
					Collector<ArrayList<Long>> outEdgesAndVertices)
							throws Exception {
				//For each property (key-value pair) appeared in query
				for(Map.Entry<String, String> propInQuery : props.entrySet()) {
					//If the vertex does not contain the specific key
					if(vertex.getProps().get(propInQuery.getKey()) == null || 
							//If the key is contained, check if the value is consistent or not
							!vertex.getProps().get(propInQuery.getKey()).equals(propInQuery.getValue())) {
						outEdgesAndVertices.collect(edgesAndVertices);
						return;
					}
				}
			}	
	}

	//select all vertices by both labels and properties
	public DataSet<ArrayList<Long>> selectVertices(int col, HashSet<String> labs, 
			HashMap<String, String> props){
		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
		DataSet<ArrayList<Long>> selectedResults = paths
			//Join with the vertices in the input graph then filter these vertices based on properties and labels
			.join(graph.getVertices())
			.where(verticesSelector)
			.equalTo(0)
			.with(new JoinAndFilterVertices(labs, props));
		
		this.paths = selectedResults;
		return selectedResults;
	}
	
	private static class JoinAndFilterVertices implements
		FlatJoinFunction<ArrayList<Long>, VertexExtended<Long, HashSet<String>,
		HashMap<String, String>>, ArrayList<Long>> {
		private HashSet<String> labs;
		private HashMap<String, String> props;
		JoinAndFilterVertices(HashSet<String> labels, HashMap<String, String> properties) { 
			this.labs = labels;
			this.props = properties;
		}

		@Override
		public void join(
				ArrayList<Long> edgesAndVertices,
				VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex,
				Collector<ArrayList<Long>> outEdgesAndVertices)
						throws Exception {
			//For each property (key-value pair) appeared in the query
			for(Map.Entry<String, String> propInQuery : props.entrySet()) {
				//If the vertex does not contain the specific key
				if(vertex.getProps().get(propInQuery.getKey()) == null || 
						//If the key is contained, check if the value is consistent or not
						!vertex.getProps().get(propInQuery.getKey()).equals(propInQuery.getValue())) 
					return;
			}
			//Check if the labels mentioned in the query are contained in the vertex label list
			if(vertex.getLabels().containsAll(labs))
				outEdgesAndVertices.collect(edgesAndVertices);
		}	
	}
	
	//No specific requirements on selected edges on right side
	public DataSet<ArrayList<Long>> selectEdgesOnRightSide(int col){
		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
		DataSet<ArrayList<Long>> selectedResults = paths
				.join(graph.getEdges())
				.where(verticesSelector)
				.equalTo(1)
				.with(new JoinEdgesOnRightSide());
		this.paths = selectedResults;
		return selectedResults;
	}

	//No specific requirements on edges
	private static class JoinEdgesOnRightSide implements
		JoinFunction<ArrayList<Long>, EdgeExtended<Long, Long, String,
		HashMap<String, String>>, ArrayList<Long>> {

		@Override
		public ArrayList<Long> join(
				ArrayList<Long> vertexAndEdgeIds,
				EdgeExtended<Long, Long, String, HashMap<String, String>> edge)throws Exception {
			vertexAndEdgeIds.add(edge.f0);
			vertexAndEdgeIds.add(edge.f2);
			return vertexAndEdgeIds;
		}	
	}
	
	//No specific requirements on selected edges on left side
		public DataSet<ArrayList<Long>> selectEdgesOnLeftSide(int col){
			KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
			DataSet<ArrayList<Long>> selectedResults = paths
					.join(graph.getEdges())
					.where(verticesSelector)
					.equalTo(2)
					.with(new JoinEdgesOnLeftSide());
			this.paths = selectedResults;
			return selectedResults;
		}
	
		//No specific requirements on edges
		private static class JoinEdgesOnLeftSide implements
			JoinFunction<ArrayList<Long>, EdgeExtended<Long, Long, String,
			HashMap<String, String>>, ArrayList<Long>> {

			@Override
			public ArrayList<Long> join(
					ArrayList<Long> vertexAndEdgeIds,
					EdgeExtended<Long, Long, String, HashMap<String, String>> edge)throws Exception {
				vertexAndEdgeIds.add(edge.f0);
				vertexAndEdgeIds.add(edge.f1);
			return vertexAndEdgeIds;
			}	
		}
	
	//Select edges By Label on right side
	public DataSet<ArrayList<Long>> selectEdgesByLabelOnRightSide(int col, String label){
		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
		DataSet<ArrayList<Long>> selectedResults = paths
				.join(graph.getEdges())
				.where(verticesSelector)
				.equalTo(1)
				.with(new JoinEdgesByLabelOnRightSide(label));
		this.paths = selectedResults;
		return selectedResults;
	}
	
	private static class JoinEdgesByLabelOnRightSide implements FlatJoinFunction<ArrayList<Long>,
			EdgeExtended<Long, Long, String, HashMap<String, String>>, ArrayList<Long>> {
		
		private String label;
		public JoinEdgesByLabelOnRightSide(String label) { this.label = label; }
		@Override
		public void join(
				ArrayList<Long> vertexAndEdgeIds,
				EdgeExtended<Long, Long, String, HashMap<String, String>> edge,
				Collector<ArrayList<Long>> outEdgesAndVertices)
				throws Exception {
			if(edge.f3.equals(this.label)) {
				vertexAndEdgeIds.add(edge.f0);
				vertexAndEdgeIds.add(edge.f2);
				outEdgesAndVertices.collect(vertexAndEdgeIds);
			}
		}		
	}
	
	//Select edges By Label on left side
		public DataSet<ArrayList<Long>> selectEdgesByLabelOnLeftSide(int col, String label){
			KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
			DataSet<ArrayList<Long>> selectedResults = paths
					.join(graph.getEdges())
					.where(verticesSelector)
					.equalTo(2)
					.with(new JoinEdgesByLabelOnLeftSide(label));
			this.paths = selectedResults;
			return selectedResults;
		}
		
		private static class JoinEdgesByLabelOnLeftSide implements FlatJoinFunction<ArrayList<Long>,
				EdgeExtended<Long, Long, String, HashMap<String, String>>, ArrayList<Long>> {
			
			private String label;
			public JoinEdgesByLabelOnLeftSide(String label) { this.label = label; }
			@Override
			public void join(
					ArrayList<Long> vertexAndEdgeIds,
					EdgeExtended<Long, Long, String, HashMap<String, String>> edge,
					Collector<ArrayList<Long>> outEdgesAndVertices)
					throws Exception {
				if(edge.f3.equals(this.label)) {
					vertexAndEdgeIds.add(edge.f0);
					vertexAndEdgeIds.add(edge.f1);
					outEdgesAndVertices.collect(vertexAndEdgeIds);
				}
			}		
		}
	
	//Select edges not including the label 
	public DataSet<ArrayList<Long>> selectReverseEdgesByLabel(int col, String label){
		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
		DataSet<ArrayList<Long>> selectedResults = paths
				.join(graph.getEdges())
				.where(verticesSelector)
				.equalTo(1)
				.with(new JoinAndFilterReverseEdgesByLabel(label));
		this.paths = selectedResults;
		return selectedResults;
	}
	
	private static class JoinAndFilterReverseEdgesByLabel implements FlatJoinFunction<ArrayList<Long>,
			EdgeExtended<Long, Long, String, HashMap<String, String>>, ArrayList<Long>> {
		
		private String label;
		public JoinAndFilterReverseEdgesByLabel(String label) { this.label = label; }
		@Override
		public void join(
				ArrayList<Long> vertexAndEdgeIds,
				EdgeExtended<Long, Long, String, HashMap<String, String>> edge,
				Collector<ArrayList<Long>> outEdgesAndVertices)
				throws Exception {
			if(!edge.f3.equals(this.label)) {
				vertexAndEdgeIds.add(edge.f0);
				vertexAndEdgeIds.add(edge.f2);
				outEdgesAndVertices.collect(vertexAndEdgeIds);
			}
		}		
	}
	
	//Select edges by their properties on right side
	public DataSet<ArrayList<Long>> selectEdgesByPropertiesOnRightSide(int col, HashMap<String, String> props){
		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
		DataSet<ArrayList<Long>> selectedResults = paths
				.join(graph.getEdges())
				.where(verticesSelector)
				.equalTo(1)
				.with(new JoinEdgesByPropertiesOnRightSide(props));
		this.paths = selectedResults;
		return selectedResults;
	}
	
	private static class JoinEdgesByPropertiesOnRightSide implements FlatJoinFunction<ArrayList<Long>,
			EdgeExtended<Long, Long, String, HashMap<String, String>>, ArrayList<Long>> {
		private HashMap<String, String> props;
		public JoinEdgesByPropertiesOnRightSide(HashMap<String, String> properties) { this.props = properties; }
		@Override
		public void join(
				ArrayList<Long> vertexAndEdgeIds,
				EdgeExtended<Long, Long, String, HashMap<String, String>> edge,
				Collector<ArrayList<Long>> outEdgesAndVertices)
				throws Exception {
			for(Map.Entry<String, String> propInQuery : props.entrySet()) {
				//If the vertex does not contain the specific key
				if(edge.getProps().get(propInQuery.getKey()) == null || 
						//If the key is contained, check if the value is consistent or not
						!edge.getProps().get(propInQuery.getKey()).equals(propInQuery.getValue())) 
					return;
			}
			vertexAndEdgeIds.add(edge.f0);
			vertexAndEdgeIds.add(edge.f2);
			outEdgesAndVertices.collect(vertexAndEdgeIds);
		}
	}
	
	//Select edges by their properties on left side
	public DataSet<ArrayList<Long>> selectEdgesByPropertiesOnLeftSide(int col, HashMap<String, String> props){
		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
		DataSet<ArrayList<Long>> selectedResults = paths
				.join(graph.getEdges())
				.where(verticesSelector)
				.equalTo(2)
				.with(new JoinEdgesByPropertiesOnLeftSide(props));
		this.paths = selectedResults;
		return selectedResults;
	}
		
	private static class JoinEdgesByPropertiesOnLeftSide implements FlatJoinFunction<ArrayList<Long>,
			EdgeExtended<Long, Long, String, HashMap<String, String>>, ArrayList<Long>> {
		private HashMap<String, String> props;
		public JoinEdgesByPropertiesOnLeftSide(HashMap<String, String> properties) { this.props = properties; }
		@Override
		public void join(
				ArrayList<Long> vertexAndEdgeIds,
				EdgeExtended<Long, Long, String, HashMap<String, String>> edge,
				Collector<ArrayList<Long>> outEdgesAndVertices) 
				throws Exception {
			for(Map.Entry<String, String> propInQuery : props.entrySet()) {
				//If the vertex does not contain the specific key
				if(edge.getProps().get(propInQuery.getKey()) == null || 
						//If the key is contained, check if the value is consistent or not
						!edge.getProps().get(propInQuery.getKey()).equals(propInQuery.getValue())) 
					return;
			}
			vertexAndEdgeIds.add(edge.f0);
			vertexAndEdgeIds.add(edge.f1);
			outEdgesAndVertices.collect(vertexAndEdgeIds);
		}
	}	
	
	//Select edges not including the properties
	public DataSet<ArrayList<Long>> selectReverseEdgesByProperties(int col, HashMap<String, String> props) {
		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
		DataSet<ArrayList<Long>> selectedResults = paths
				.join(graph.getEdges())
				.where(verticesSelector)
				.equalTo(1)
				.with(new JoinAndFilterReverseEdgesByProperties(props));
		this.paths = selectedResults;
		return selectedResults;
	}
		
	private static class JoinAndFilterReverseEdgesByProperties implements FlatJoinFunction<ArrayList<Long>,
		EdgeExtended<Long, Long, String, HashMap<String, String>>, ArrayList<Long>> {
			
		private HashMap<String, String> props;
		public JoinAndFilterReverseEdgesByProperties(HashMap<String, String> properties) { this.props = properties; }
			
		@Override
		public void join(
				ArrayList<Long> vertexAndEdgeIds,
				EdgeExtended<Long, Long, String, HashMap<String, String>> edge,
				Collector<ArrayList<Long>> outEdgesAndVertices)
					throws Exception {
			for(Map.Entry<String, String> propInQuery : props.entrySet()) {
					//If the vertex does not contain the specific key
				if(edge.getProps().get(propInQuery.getKey()) == null || 
							//If the key is contained, check if the value is consistent or not
						!edge.getProps().get(propInQuery.getKey()).equals(propInQuery.getValue())) {
					outEdgesAndVertices.collect(vertexAndEdgeIds);
					return;
				}
			}
		}
	}
	
	//return all vertices specified by their IDs in a column
	public DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> projectDistinctVertices(int col){
		DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> returnedVertices = paths
				.map(new ExtractVertexIds(col))
				.distinct()
				.join(graph.getVertices())
				.where(0)
				.equalTo(0)
				.with(new ProjectSelectedVertices());
		return returnedVertices;
	}
	
	private static class ExtractVertexIds implements MapFunction<ArrayList<Long>, Tuple1<Long>> {

		private int column;
		public ExtractVertexIds(int col) { this.column = col; }
		@Override
		public Tuple1<Long> map(ArrayList<Long> vertex) throws Exception {
			return new Tuple1<Long>(vertex.get(this.column));			
		}
	}
	
	
	private static class ProjectSelectedVertices implements JoinFunction<Tuple1<Long>,
		VertexExtended<Long, HashSet<String>, HashMap<String, String>>, VertexExtended<Long, HashSet<String>, HashMap<String, String>>> {

		@Override
		public VertexExtended<Long, HashSet<String>, HashMap<String, String>> join(
				Tuple1<Long> vertexIds,
				VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex)
				throws Exception {
			return vertex;
		}
	}
	

	//return all edges specified by their IDs in a column
	public DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> projectDistinctEdges(int col){
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> returnedVertices = paths
				.map(new ExtractEdgeIds(col))
				.distinct()
				.join(graph.getEdges())
				.where(0)
				.equalTo(0)
				.with(new ProjectSelectedEdges());
		return returnedVertices;
	}
	
	private static class ExtractEdgeIds implements MapFunction<ArrayList<Long>, Tuple1<Long>> {

		private int column;
		public ExtractEdgeIds(int col) { this.column = col; }
		@Override
		public Tuple1<Long> map(ArrayList<Long> edge) throws Exception {
			return new Tuple1<Long>(edge.get(this.column));			
		}
	}
	
	
	private static class ProjectSelectedEdges implements JoinFunction<Tuple1<Long>,
		EdgeExtended<Long, Long, String, HashMap<String, String>>, EdgeExtended<Long, Long, String, HashMap<String, String>>> {

		@Override
		public EdgeExtended<Long, Long, String, HashMap<String, String>> join(
				Tuple1<Long> vertexIds,
				EdgeExtended<Long, Long, String, HashMap<String, String>> edge)
				throws Exception {
			return edge;
		}
	}

}
