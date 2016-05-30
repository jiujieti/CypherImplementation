package operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
@SuppressWarnings("serial")

public class LabelMatchingOperatorsBeta {
	//Input graph
	private final GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long,
	  String, HashMap<String, String>> graph;
		
	//Each list contains the vertex IDs and edge IDs of a selected path so far 
	private DataSet<ArrayList<Long>> paths;

	//Get the input graph, current columnNumber and the vertex and edges IDs
	public LabelMatchingOperatorsBeta(GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long,
			  String, HashMap<String, String>> graph,
			  DataSet<ArrayList<Long>> paths) {
		this.graph = graph;
		this.paths = paths;
	}
	
	public DataSet<Tuple2<Long, Long>> matchWithUpperBound(int col, int ub, String label, JoinHint strategy) throws Exception {
		//Initial WorkSet DataSet consisting of vertex-pair IDs for Delta Iteration. Each field of Tuple2<Long, Long> stores two same IDs since these two are starting vertices
		DataSet<Tuple2<Long, Long>> initialSolutionSet = this.paths
				.map(new ExtractVertexIds(col));
		
		int maxIterations = 7;
		//int keyPosition = 1;
		
		DataSet<Tuple2<Long, Long>> initialWorkSet = initialSolutionSet;
		
		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration = initialSolutionSet
			    .iterateDelta(initialWorkSet, maxIterations, 0);
		
		DataSet<Tuple2<Long,Long>> newResults = iteration
				.getWorkset()
				.join(graph.getEdges(), strategy)
				.where(1)
				.equalTo(1)
				.with(new FilterEdgesByLabel(label));

		DataSet<Tuple2<Long,Long>> deltas = iteration
				.getSolutionSet()
				.coGroup(newResults)
				.where(0)
				.equalTo(0)
				.with(new UpdateResults())
				.withForwardedFieldsFirst("0")
				.withForwardedFieldsSecond("0");	
		
		DataSet<Tuple2<Long, Long>> nextWorkset = iteration
				.getSolutionSet()
				.coGroup(newResults)
				.where(0)
				.equalTo(0)
				.with(new GetNewResults());
	

		DataSet<Tuple2<Long, Long>> mergedResults = iteration.closeWith(deltas, nextWorkset);
		

		return mergedResults;

		
		
	} 

	private static class ExtractVertexIds implements MapFunction<ArrayList<Long>, 
		Tuple2<Long, Long>> {
		
		private int col = 0;
		
		ExtractVertexIds(int column) { this.col = column; }
		
		@Override
		public Tuple2<Long, Long> map(ArrayList<Long> idsOfVerticesAndEdges)
				throws Exception {
			return new Tuple2<Long, Long>(idsOfVerticesAndEdges.get(col), idsOfVerticesAndEdges.get(col));
		}		
	}
	
	private static class FilterEdgesByLabel implements FlatJoinFunction<Tuple2<Long, Long>, 
	EdgeExtended<Long, Long, String, HashMap<String, String>>, Tuple2<Long, Long>> {

		private String lab = "";
		FilterEdgesByLabel(String label) { this.lab = label; }
		@Override
		public void join(
				Tuple2<Long, Long> vertexIds,
				EdgeExtended<Long, Long, String, HashMap<String, String>> edge,
				Collector<Tuple2<Long, Long>> out) throws Exception {
			if(edge.f3.equals(lab))
				out.collect(new Tuple2<Long, Long>(vertexIds.f0, edge.f2));			
		}
	}

	
	

	private static class GetNewResults implements CoGroupFunction<Tuple2<Long, Long>, 
		Tuple2<Long, Long>, Tuple2<Long, Long>> {
		
		@Override
		
		public void coGroup(Iterable<Tuple2<Long, Long>> originalVertexIds,
				Iterable<Tuple2<Long, Long>> newVertexIds,
		Collector<Tuple2<Long, Long>> results) throws Exception {
			HashSet<Tuple2<Long, Long>> prevResults = new HashSet<Tuple2<Long,Long>>();
			for (Tuple2<Long, Long> prev : originalVertexIds) {
				prevResults.add(prev);
			}
			for (Tuple2<Long, Long> next: newVertexIds) {
				if (!prevResults.contains(next)) {
					results.collect(next);
				}
			}
		}
	}
	
	private static class UpdateResults implements CoGroupFunction<Tuple2<Long, Long>,
		Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public void coGroup(Iterable<Tuple2<Long, Long>> addedVertexIds,
				Iterable<Tuple2<Long, Long>> prevVertexIds,
				Collector<Tuple2<Long, Long>> results) throws Exception {
			for(Tuple2<Long, Long> prev: prevVertexIds) {
				results.collect(prev);	
			}
			for(Tuple2<Long, Long> added: addedVertexIds) {
				results.collect(added);	
			}
			
		}
		
	}
	

		
}
