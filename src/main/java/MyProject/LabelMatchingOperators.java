package MyProject;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import MyProject.DataStructures.GraphExtended;
import MyProject.DataStructures.EdgeExtended;
import MyProject.KeySelectorForVertices;

@SuppressWarnings("serial")
public class LabelMatchingOperators {
	/*Input graph*/
	private GraphExtended<Long, ArrayList<String>, HashMap<String, String>, String,
	  String, HashMap<String, String>> graph;
	
	/*Each list contains the vertex IDs and edge IDs of a selected path so far */
	private DataSet<ArrayList<Tuple2<String, Long>>> vertexAndEdgeIds;

	/*Get the input graph, current columnNumber and the vertex and edges IDs*/
	public LabelMatchingOperators(GraphExtended<Long, ArrayList<String>, HashMap<String, String>, String,
			  String, HashMap<String, String>> g,
			  DataSet<ArrayList<Tuple2<String, Long>>> verticesAndEdges) {
		this.graph = g;
		this.vertexAndEdgeIds = verticesAndEdges;
	}
	
	public void matchWithBounds(int col, int lb, int ub, String label) {
		
	} 
	
	public void matchWithUpperBound(int col, int ub, String label) {
		
	} 
	
	public void matchWithLowerBound(int col, int lb, String label) {
		
	}

	/*Return all (source vertex, target vertex) pairs according to unbounded label propogation*/
	public DataSet<Tuple2<Long, Long>> matchWithoutBounds(int col, String label) throws Exception {
		/*Initial WorkSet DataSet consisting of vertex-pair IDs for Delta Iteration
		 *Each field of Tuple2<Long, Long> stores two same IDs since these two are starting vertices*/
		DataSet<Tuple2<Long, Long>> initialVertexIds = this.vertexAndEdgeIds
				.map(new ExtractVertexIds(col));
		
		/*No bound restrictions, starting vertices are also part of the solution*/
		DataSet<Tuple2<Long, Long>> initialSolutionSet = initialVertexIds;
		
	
		/*Unlimited propagation steps, this number needs to be modified later*/
		int maxIterations = 1;
		
		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration = 
				initialSolutionSet
					.iterateDelta(initialVertexIds, maxIterations, 0);
		
		/*return the new work set consisting of vertex Ids
		 * edges are filtered by input label*/
		DataSet<Tuple2<Long, Long>> nextWorkset = iteration.getWorkset()
					.join(graph.getEdges())
					.where(1)
					.equalTo(1)
					.with(new FilterEdgesByLabel(label));
		
		/*Next solution*/
		DataSet<Tuple2<Long, Long>> deltas = nextWorkset;
		
		
		DataSet<Tuple2<Long, Long>> allVertexPairs = iteration.closeWith(deltas, nextWorkset);

		
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
		
		//this.vertexAndEdgeIds = results;
		return allVertexPairs;
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

		private String lab = null;
		FilterEdgesByLabel(String label) { this.lab = label; }
		@Override
		public void join(
				Tuple2<Long, Long> vertexIds,
				EdgeExtended<String, Long, String, HashMap<String, String>> edge,
				Collector<Tuple2<Long, Long>> out) throws Exception {
			if(edge.f3.equals(lab) && !vertexIds.f0.equals(vertexIds.f1))
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
