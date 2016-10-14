package operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
@SuppressWarnings("serial")


/*
* A more efficient way to implement label matching operator by using delta iterator in Flink
* So far only work with non-circle query pattern
* */
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
	
    //Only the one with upper bound is implemented
	public DataSet<ArrayList<Long>> matchWithUpperBound(int col, int ub, String label, JoinHint strategy) throws Exception {
		//Initial WorkSet DataSet consisting of vertex-pair IDs for Delta Iteration. Each field of Tuple2<Long, Long> stores two same IDs since these two are starting vertices
		DataSet<Tuple2<Long, Tuple2<Long, Long>>> initialSolutionSet = this.paths
				.map(new ExtractVertexIds(col));

		int maxIterations = ub;

		DataSet<Tuple2<Long, Tuple2<Long, Long>>> initialWorkSet = initialSolutionSet;

		DeltaIteration<Tuple2<Long, Tuple2<Long, Long>>, Tuple2<Long, Tuple2<Long, Long>>> iteration = initialSolutionSet.iterateDelta(initialWorkSet, maxIterations, 1);
		DataSet<Tuple2<Long, Tuple2<Long, Long>>> nextWorkset = iteration
				.getWorkset()
				.join(graph.getEdges(), strategy)
				.where(0)
				.equalTo(1)
				.with(new FilterEdgesByLabel(label));

		DataSet<Tuple2<Long, Tuple2<Long, Long>>> deltas = iteration
				.getSolutionSet()
				.coGroup(nextWorkset)
				.where(1)
				.equalTo(1)
                .with(new GetNewResults());

		DataSet<Tuple2<Long, Tuple2<Long, Long>>> mergedResults = iteration.closeWith(deltas, nextWorkset);

	    KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
	    DataSet<ArrayList<Long>> results = this.paths
				.join(mergedResults.map(new ExtractVertexPairs()))
				.where(verticesSelector)
				.equalTo(0)
				.with(new UpdateVertexAndEdgeIds());

		this.paths = results;

		return results;

	}

	private static class ExtractVertexIds implements MapFunction<ArrayList<Long>,
			Tuple2<Long, Tuple2<Long, Long>>>{
		private int col = 0;
		ExtractVertexIds(int column) { this.col = column; }
		@Override
		public Tuple2<Long, Tuple2<Long, Long>> map(ArrayList<Long> idsOfVerticesAndEdges)
				throws Exception {
			return new Tuple2<Long, Tuple2<Long, Long>>(idsOfVerticesAndEdges.get(col), new Tuple2<Long, Long>(idsOfVerticesAndEdges.get(col), idsOfVerticesAndEdges.get(col)));
		}
	}

	private static class FilterEdgesByLabel implements FlatJoinFunction<Tuple2<Long, Tuple2<Long, Long>>, EdgeExtended<Long, Long, String, HashMap<String, String>>,
		Tuple2<Long, Tuple2<Long, Long>>> {
		private String label;

		public FilterEdgesByLabel(String label) {this.label = label;}

		@Override
		public void join(
				Tuple2<Long, Tuple2<Long, Long>> vertexIds,
				EdgeExtended<Long, Long, String, HashMap<String, String>> edge,
				Collector<Tuple2<Long, Tuple2<Long, Long>>> vertices)
				throws Exception {
			if(edge.f3.equals(this.label)){
				Tuple2<Long, Long> result = new Tuple2<>(vertexIds.f1.f0, edge.f2);
				vertices.collect(new Tuple2<Long, Tuple2<Long, Long>>(edge.f2, result));
			}
		}
    }

	private static class GetNewResults implements CoGroupFunction<Tuple2<Long, Tuple2<Long, Long>>, Tuple2<Long, Tuple2<Long, Long>>, Tuple2<Long, Tuple2<Long, Long>>> {

		@Override
		public void coGroup(Iterable<Tuple2<Long, Tuple2<Long, Long>>> originalVertices,
				Iterable<Tuple2<Long, Tuple2<Long, Long>>> newResults,
				Collector<Tuple2<Long, Tuple2<Long, Long>>> vertices)
				throws Exception {
			HashSet<Tuple2<Long, Tuple2<Long, Long>>> prevVertices = new HashSet<>();
			for (Tuple2<Long, Tuple2<Long, Long>> prev : originalVertices) {
				prevVertices.add(prev);
			}
			for (Tuple2<Long, Tuple2<Long, Long>> next: newResults) {
				if (!prevVertices.contains(next)) {
					vertices.collect(next);
				}
			}
		}
	}

	private static class UpdateVertexAndEdgeIds implements FlatJoinFunction<ArrayList<Long>, Tuple2<Long, Long>, ArrayList<Long>> {

		@Override
		public void join(ArrayList<Long> vertexAndEdgeIds,
				Tuple2<Long, Long> vertexIds,
				Collector<ArrayList<Long>> updateIdsList) throws Exception {
			vertexAndEdgeIds.add(vertexIds.f1);
			updateIdsList.collect(vertexAndEdgeIds);
		}
	}

	private static class ExtractVertexPairs implements MapFunction<Tuple2<Long, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> map(Tuple2<Long, Tuple2<Long, Long>> vertexIds)
				throws Exception {
			return vertexIds.f1;
		}

	}
}
