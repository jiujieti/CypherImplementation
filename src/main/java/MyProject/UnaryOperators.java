package MyProject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import MyProject.DataStructures.*;
import MyProject.KeySelectorForVertices;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class UnaryOperators {
	//Input graph
	private GraphExtended<Long, ArrayList<String>, HashMap<String, String>, String,
	  String, HashMap<String, String>> graph;
	
	//Each list contains the vertex IDs and edge IDs of a selected path so far 
	private DataSet<ArrayList<Tuple2<String, Long>>> vertexAndEdgeIds;
	
	//Get the input graph, current columnNumber and the vertex and edges IDs
	public UnaryOperators(GraphExtended<Long, ArrayList<String>, HashMap<String, String>, String,
			  String, HashMap<String, String>> g,
			  DataSet<ArrayList<Tuple2<String, Long>>> verticesAndEdges) {
		this.graph = g;
		this.vertexAndEdgeIds = verticesAndEdges;
	}
	
	//No specific queries on the current vertices
	public DataSet<ArrayList<Tuple2<String, Long>>> selectVertices() {
		return vertexAndEdgeIds;
	}
	
	//Select all vertices by their labels
	public DataSet<ArrayList<Tuple2<String, Long>>> selectVerticesByLabels(ArrayList<String> labs, int col){
		KeySelectorForVertices verticesSelector = new KeySelectorForVertices(col);
		DataSet<ArrayList<Tuple2<String, Long>>> selectedResults = vertexAndEdgeIds
			//Group the list according to the current position of the vertices to be processed
			.groupBy(verticesSelector)
			//Transfer the unsorted grouping into DataSet
			 .getDataSet()
			 //Join with the vertices in the input graph then filter these vertices based on labels
			 .join(graph.getVertices())
			 .where(verticesSelector)
			 .equalTo(0)
			 .with(new JoinAndFilterVerticesByLabels(labs));
		
		this.vertexAndEdgeIds = selectedResults;
		return selectedResults;
	}
	
	private static class JoinAndFilterVerticesByLabels implements
			FlatJoinFunction<ArrayList<Tuple2<String, Long>>, VertexExtended<Long, ArrayList<String>,
			HashMap<String, String>>, ArrayList<Tuple2<String, Long>>> {

		private ArrayList<String> labels;
		JoinAndFilterVerticesByLabels(ArrayList<String> labelList) { this.labels = labelList;}
		@Override
		public void join(
				ArrayList<Tuple2<String, Long>> edgesAndVertices,
				VertexExtended<Long, ArrayList<String>, HashMap<String, String>> vertex,
				Collector<ArrayList<Tuple2<String, Long>>> outEdgesAndVertices)
				throws Exception {
					//Check if the labels mentioned in the query are contained in the vertex label list
					if(vertex.getLabels().containsAll(labels))
						outEdgesAndVertices.collect(edgesAndVertices);
		}	
	}
		
	public DataSet<ArrayList<Tuple2<String, Long>>> selectVerticesByProperties(HashMap<String, String> props, int col){
		KeySelectorForVertices verticesSelector = new KeySelectorForVertices(col);
		DataSet<ArrayList<Tuple2<String, Long>>> selectedResults = vertexAndEdgeIds
			//Group the list according to the current position of the vertices to be processed
			.groupBy(verticesSelector)
			//Transfer the unsorted grouping into DataSet
			.getDataSet()
			//Join with the vertices in the input graph then filter these vertices based on properties
			.join(graph.getVertices())
			.where(verticesSelector)
			.equalTo(0)
			.with(new JoinAndFilterVerticesByProperties(props));
		
		this.vertexAndEdgeIds = selectedResults;
		return selectedResults;
	}
	
	private static class JoinAndFilterVerticesByProperties implements
		FlatJoinFunction<ArrayList<Tuple2<String, Long>>, VertexExtended<Long, ArrayList<String>,
			HashMap<String, String>>, ArrayList<Tuple2<String, Long>>> {

			private HashMap<String, String> props;
			JoinAndFilterVerticesByProperties(HashMap<String, String> properties) { this.props = properties;}

			@Override
			public void join(
					ArrayList<Tuple2<String, Long>> edgesAndVertices,
					VertexExtended<Long, ArrayList<String>, HashMap<String, String>> vertex,
					Collector<ArrayList<Tuple2<String, Long>>> outEdgesAndVertices)
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
	
	public DataSet<ArrayList<Tuple2<String, Long>>> selectVertices(ArrayList<String> labs, 
			HashMap<String, String> props, int col){
		KeySelectorForVertices verticesSelector = new KeySelectorForVertices(col);
		DataSet<ArrayList<Tuple2<String, Long>>> selectedResults = vertexAndEdgeIds
			//Group the list according to the current position of the vertices to be processed
			.groupBy(verticesSelector)
			//Transfer the unsorted grouping into DataSet
			.getDataSet()
			//Join with the vertices in the input graph then filter these vertices based on properties
			.join(graph.getVertices())
			.where(verticesSelector)
			.equalTo(0)
			.with(new JoinAndFilterVertices(labs, props));
		
		this.vertexAndEdgeIds = selectedResults;
		return selectedResults;
	}
	
	private static class JoinAndFilterVertices implements
		FlatJoinFunction<ArrayList<Tuple2<String, Long>>, VertexExtended<Long, ArrayList<String>,
		HashMap<String, String>>, ArrayList<Tuple2<String, Long>>> {
		private ArrayList<String> labs;
		private HashMap<String, String> props;
		JoinAndFilterVertices(ArrayList<String> labels, HashMap<String, String> properties) { 
			this.labs = labels;
			this.props = properties;
		}

		@Override
		public void join(
				ArrayList<Tuple2<String, Long>> edgesAndVertices,
				VertexExtended<Long, ArrayList<String>, HashMap<String, String>> vertex,
				Collector<ArrayList<Tuple2<String, Long>>> outEdgesAndVertices)
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
	
	//No specific queries on the current edge
	public DataSet<ArrayList<Tuple2<String, Long>>> selectEdges(int col){
		KeySelectorForVertices verticesSelector = new KeySelectorForVertices(col);
		DataSet<ArrayList<Tuple2<String, Long>>> selectedResults = vertexAndEdgeIds
				.groupBy(verticesSelector)
				.getDataSet()
				.join(graph.getEdges())
				.where(verticesSelector)
				.equalTo(1)
				.with(new JoinEdges());
		this.vertexAndEdgeIds = selectedResults;
		return selectedResults;
	}
	
	
	private static class JoinEdges implements
		JoinFunction<ArrayList<Tuple2<String, Long>>, EdgeExtended<String, Long, String,
	HashMap<String, String>>, ArrayList<Tuple2<String, Long>>> {

	@Override
	public ArrayList<Tuple2<String, Long>> join(
			ArrayList<Tuple2<String, Long>> vertexAndEdgeIds,
			EdgeExtended<String, Long, String, HashMap<String, String>> edge)
			throws Exception {
			vertexAndEdgeIds.add(new Tuple2<String, Long>(edge.f0, edge.f2));
		return vertexAndEdgeIds;
		}	
	}
	
	public DataSet<ArrayList<Tuple2<String, Long>>> selectEdgesByLabel(int col, String label){
		KeySelectorForVertices verticesSelector = new KeySelectorForVertices(col);
		DataSet<ArrayList<Tuple2<String, Long>>> selectedResults = vertexAndEdgeIds
				.groupBy(verticesSelector)
				.getDataSet()
				.join(graph.getEdges())
				.where(verticesSelector)
				.equalTo(1)
				.with(new JoinEdgesByLabel(label));
		this.vertexAndEdgeIds = selectedResults;
		return selectedResults;
	}
	
	private static class JoinEdgesByLabel implements FlatJoinFunction<ArrayList<Tuple2<String, Long>>,
			EdgeExtended<String, Long, String, HashMap<String, String>>, ArrayList<Tuple2<String, Long>>> {
		
		private String label;
		public JoinEdgesByLabel(String label) { this.label = label; }
		@Override
		public void join(
				ArrayList<Tuple2<String, Long>> vertexAndEdgeIds,
				EdgeExtended<String, Long, String, HashMap<String, String>> edge,
				Collector<ArrayList<Tuple2<String, Long>>> outEdgesAndVertices)
				throws Exception {
			if(edge.f3.equals(this.label))
				outEdgesAndVertices.collect(vertexAndEdgeIds);
		}		
	}
	
	public DataSet<ArrayList<Tuple2<String, Long>>> selectEdgesByProperties(int col, HashMap<String, String> props){
		KeySelectorForVertices verticesSelector = new KeySelectorForVertices(col);
		DataSet<ArrayList<Tuple2<String, Long>>> selectedResults = vertexAndEdgeIds
				.groupBy(verticesSelector)
				.getDataSet()
				.join(graph.getEdges())
				.where(verticesSelector)
				.equalTo(1)
				.with(new JoinEdgesByProperties(props));
		this.vertexAndEdgeIds = selectedResults;
		return selectedResults;
	}
	
	private static class JoinEdgesByProperties implements FlatJoinFunction<ArrayList<Tuple2<String, Long>>,
	EdgeExtended<String, Long, String, HashMap<String, String>>, ArrayList<Tuple2<String, Long>>> {
		private HashMap<String, String> props;
		public JoinEdgesByProperties(HashMap<String, String> properties) { this.props = properties; }
		@Override
		public void join(
				ArrayList<Tuple2<String, Long>> vertexAndEdgeIds,
				EdgeExtended<String, Long, String, HashMap<String, String>> edge,
				Collector<ArrayList<Tuple2<String, Long>>> outEdgesAndVertices)
				throws Exception {
			for(Map.Entry<String, String> propInQuery : props.entrySet()) {
				//If the vertex does not contain the specific key
				if(edge.getProps().get(propInQuery.getKey()) == null || 
						//If the key is contained, check if the value is consistent or not
						!edge.getProps().get(propInQuery.getKey()).equals(propInQuery.getValue())) 
					return;
			}
			outEdgesAndVertices.collect(vertexAndEdgeIds);
		}
	}
	
}
