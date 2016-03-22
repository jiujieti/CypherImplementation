package MyProject;

import java.util.ArrayList;
import java.util.HashMap;






import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import MyProject.DataStructures.GraphExtended;
import MyProject.DataStructures.EdgeExtended;
import MyProject.KeySelectorForVertices;
/* Using for loop to do the iterations now to collect those vertex IDs
 * Some flaws are still existing here:
 * (1) Too many iterations may cause the termination due to few memory
 * (2) Check the result dataset then return "No vertices found" information
 * (3) Cycle detection (unbounded label matching could not terminate regularly)
 * (4) Duplicated code snippet (a new method to be added) 
 * 
 * */
@SuppressWarnings("serial")
public class LabelMatchingOperators {
	//Input graph
	private GraphExtended<Long, ArrayList<String>, HashMap<String, String>, String,
	  String, HashMap<String, String>> graph;
	
	//Each list contains the vertex IDs and edge IDs of a selected path so far 
	private DataSet<ArrayList<Tuple2<String, Long>>> vertexAndEdgeIds;

	//Get the input graph, current columnNumber and the vertex and edges IDs
	public LabelMatchingOperators(GraphExtended<Long, ArrayList<String>, HashMap<String, String>, String,
			  String, HashMap<String, String>> g,
			  DataSet<ArrayList<Tuple2<String, Long>>> verticesAndEdges) {
		this.graph = g;
		this.vertexAndEdgeIds = verticesAndEdges;
	}
	
	public DataSet<ArrayList<Tuple2<String, Long>>> matchWithBounds(int col, int lb, int ub, String label) throws Exception {
		//Initial WorkSet DataSet consisting of vertex-pair IDs for Delta Iteration. Each field of Tuple2<Long, Long> stores two same IDs since these two are starting vertices
		DataSet<Tuple2<Long, Long>> verticesWorkset = this.vertexAndEdgeIds
				.map(new ExtractVertexIds(col));
		
		//To get the starting workset of vertices after minimum hops for further propagations  
		for(int i = 1; i <= lb; i++) {
			verticesWorkset = verticesWorkset.join(graph.getEdges())
					.where(1)
					.equalTo(1)
					.with(new FilterEdgesByLabel(label))
					.distinct();
		}
		
		DataSet<Tuple2<Long, Long>> allVertexPairs = verticesWorkset;
		for(int j = 1; j <= ub - lb; j++) {
			verticesWorkset = verticesWorkset.join(graph.getEdges())
					.where(1)
					.equalTo(1)
					.with(new FilterEdgesByLabel(label))
					.distinct();
			if(verticesWorkset.count() == 0)
				break;
			allVertexPairs = allVertexPairs.union(verticesWorkset).distinct();
		}
		
		KeySelectorForVertices verticesSelector = new KeySelectorForVertices(col); 
		DataSet<ArrayList<Tuple2<String, Long>>> results = this.vertexAndEdgeIds
				.groupBy(verticesSelector)
				.getDataSet()
				.join(allVertexPairs)
				.where(verticesSelector)
				.equalTo(0)
				.with(new UpdateVertexAndEdgeIds());
		
		this.vertexAndEdgeIds = results;
		return results;
	} 
	
	public DataSet<ArrayList<Tuple2<String, Long>>> matchWithUpperBound(int col, int ub, String label) throws Exception {
		//Initial WorkSet DataSet consisting of vertex-pair IDs for Delta Iteration. Each field of Tuple2<Long, Long> stores two same IDs since these two are starting vertices
		DataSet<Tuple2<Long, Long>> verticesWorkset = this.vertexAndEdgeIds
				.map(new ExtractVertexIds(col));
		
		DataSet<Tuple2<Long, Long>> allVertexPairs = verticesWorkset;
		for(int i = 1; i <= ub; i++) {
			verticesWorkset = verticesWorkset.join(graph.getEdges())
					.where(1)
					.equalTo(1)
					.with(new FilterEdgesByLabel(label))
					.distinct();
			if(verticesWorkset.count() == 0)
				break;
			allVertexPairs = allVertexPairs.union(verticesWorkset).distinct();
		}
		
		KeySelectorForVertices verticesSelector = new KeySelectorForVertices(col); 
		DataSet<ArrayList<Tuple2<String, Long>>> results = this.vertexAndEdgeIds
				.groupBy(verticesSelector)
				.getDataSet()
				.join(allVertexPairs)
				.where(verticesSelector)
				.equalTo(0)
				.with(new UpdateVertexAndEdgeIds());
		
		this.vertexAndEdgeIds = results;
		return results;
	} 
	
	public DataSet<ArrayList<Tuple2<String, Long>>> matchWithLowerBound(int col, int lb, String label) throws Exception {
		DataSet<Tuple2<Long, Long>> verticesWorkset = this.vertexAndEdgeIds
				.map(new ExtractVertexIds(col));
		//To get the starting workset of vertices after minimum hops for further propagations  
		for(int i = 1; i <= lb; i++) {
			verticesWorkset = verticesWorkset.join(graph.getEdges())
					.where(1)
					.equalTo(1)
					.with(new FilterEdgesByLabel(label))
					.distinct();
		}
		DataSet<Tuple2<Long, Long>> allVertexPairs = verticesWorkset;
		for(int j = 1; j <= 1000; j++) {
			verticesWorkset = verticesWorkset.join(graph.getEdges())
					.where(1)
					.equalTo(1)
					.with(new FilterEdgesByLabel(label))
					.distinct();
			if(verticesWorkset.count() == 0)
				break;
			allVertexPairs = allVertexPairs.union(verticesWorkset).distinct();
		}
		
		KeySelectorForVertices verticesSelector = new KeySelectorForVertices(col); 
		DataSet<ArrayList<Tuple2<String, Long>>> results = this.vertexAndEdgeIds
				.groupBy(verticesSelector)
				.getDataSet()
				.join(allVertexPairs)
				.where(verticesSelector)
				.equalTo(0)
				.with(new UpdateVertexAndEdgeIds());
		
		this.vertexAndEdgeIds = results;
		return results;
	}

	//Return all (source vertex, target vertex) pairs according to unbounded label propogation
	public DataSet<ArrayList<Tuple2<String, Long>>> matchWithoutBounds(int col, String label) throws Exception {
		//Initial WorkSet DataSet consisting of vertex-pair IDs for Delta Iteration Each field of Tuple2<Long, Long> stores two same IDs since these two are starting vertices
		DataSet<Tuple2<Long, Long>> verticesWorkset = this.vertexAndEdgeIds
				.map(new ExtractVertexIds(col));
			
		int minIterations = 1;
		int maxIterations = 7;
		DataSet<Tuple2<Long, Long>> allVertexPairs = verticesWorkset;
		for(int i = minIterations; i <= maxIterations; i++) {
			verticesWorkset = verticesWorkset.join(graph.getEdges())
					.where(1)
					.equalTo(1)
					.with(new FilterEdgesByLabel(label))
					.distinct();
			if(verticesWorkset.count() == 0)
				break;
			allVertexPairs = allVertexPairs.union(verticesWorkset).distinct();
			
		}
		
		KeySelectorForVertices verticesSelector = new KeySelectorForVertices(col); 
		DataSet<ArrayList<Tuple2<String, Long>>> results = this.vertexAndEdgeIds
				.groupBy(verticesSelector)
				.getDataSet()
				.join(allVertexPairs)
				.where(verticesSelector)
				.equalTo(0)
				.with(new UpdateVertexAndEdgeIds());
		
		this.vertexAndEdgeIds = results;
		return results;
		/*No bound restrictions, starting vertices are also part of the solution*/
		//DataSet<Tuple2<Long, Long>> initialSolutionSet = initialVertexIds;
		
	
		/*Unlimited propagation steps, this number needs to be modified later*/
		//int maxIterations = 78;
		
		/*DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration = 
				initialSolutionSet
					.iterateDelta(initialVertexIds, maxIterations, 1);*/
		
		/*return the new work set consisting of vertex Ids
		 * edges are filtered by input label*/
		/*DataSet<Tuple2<Long, Long>> nextWorkset = iteration.getWorkset()
					.join(graph.getEdges())
					.where(1)
					.equalTo(1)
					.with(new FilterEdgesByLabel(label));*/
		
		/*Next solution*/
		/*DataSet<Tuple2<Long, Long>> deltas = iteration.getSolutionSet()
				.coGroup(iteration.getWorkset())
				.where(1)
				.equalTo(0)
				.with(new KeepVertexIds());*/
		
		
		//DataSet<Tuple2<Long, Long>> allVertexPairs = iteration.closeWith(deltas, nextWorkset);

		
		/*Update the results to ArrayList storing vertex and edge IDs 
		 * the edge ID will be set to null*/
		/*KeySelectorForVertices verticesSelector = new KeySelectorForVertices(col); 
		DataSet<ArrayList<Tuple2<String, Long>>> results = this.vertexAndEdgeIds
				.groupBy(verticesSelector)
				.getDataSet()
				.join(allVertexPairs)
				.where(verticesSelector)
				.equalTo(0)
				.with(new UpdateVertexAndEdgeIds());*/
		
		//
		
	}
	
	private static class ExtractVertexIds implements MapFunction<ArrayList<Tuple2<String, Long>>, 
			Tuple2<Long, Long>> {
		private int col = 0;
		ExtractVertexIds(int column) { this.col = column; }
		@Override
		public Tuple2<Long, Long> map(ArrayList<Tuple2<String, Long>> idsOfVerticesAndEdges)
				throws Exception {
			return new Tuple2<Long, Long>(idsOfVerticesAndEdges.get(col).f1, idsOfVerticesAndEdges.get(col).f1);
		}		
	}
		
	private static class FilterEdgesByLabel implements FlatJoinFunction<Tuple2<Long, Long>, 
			EdgeExtended<String, Long, String, HashMap<String, String>>, Tuple2<Long, Long>> {

		private String lab = "";
		FilterEdgesByLabel(String label) { this.lab = label; }
		@Override
		public void join(
				Tuple2<Long, Long> vertexIds,
				EdgeExtended<String, Long, String, HashMap<String, String>> edge,
				Collector<Tuple2<Long, Long>> out) throws Exception {
			if(edge.f3.equals(lab))
				out.collect(new Tuple2<Long, Long>(vertexIds.f0, edge.f2));			
		}
	}
	
	
	private static class UpdateVertexAndEdgeIds implements FlatJoinFunction<ArrayList<Tuple2<String, Long>>,
			Tuple2<Long, Long>, ArrayList<Tuple2<String, Long>>> {
		@Override
		public void join(
				ArrayList<Tuple2<String, Long>> vertexAndEdgeIds,
				Tuple2<Long, Long> vertexIds,
				Collector<ArrayList<Tuple2<String, Long>>> updateIdsList) throws Exception {
			vertexAndEdgeIds.add(new Tuple2<String, Long>(null, vertexIds.f1));
			updateIdsList.collect(vertexAndEdgeIds);
		}
	}
}
