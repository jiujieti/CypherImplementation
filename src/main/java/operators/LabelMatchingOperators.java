package operators;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;

import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/*
 * Label matching operators are implemented here:
 * (1）Define both the upper bound and the lower bound of the number of edges on the paths
 * (2) Define the upper bound of the number of edges on the paths
 * (3) Define the lower bound of the number of edges on the paths
 * (4) Do not define any bounds
 * */
@SuppressWarnings("serial")
public class LabelMatchingOperators {
	//Input graph
	private final GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long,
	  String, HashMap<String, String>> graph;
	
	//Each list contains the vertex IDs and edge IDs of a selected path so far 
	private DataSet<ArrayList<Long>> paths;

	//Get the input graph, current columnNumber and the vertex and edges IDs
	public LabelMatchingOperators(GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long,
			  String, HashMap<String, String>> graph,
			  DataSet<ArrayList<Long>> paths) {
		this.graph = graph;
		this.paths = paths;
	}

	//define both upper bound and lower bound of the number of edges traversed
	public DataSet<ArrayList<Long>> matchWithBounds(int col, int lb, int ub, String label, JoinHint strategy) throws Exception {

		DataSet<Tuple2<Long, Long>> verticesWorkset = this.paths
				.map(new ExtractVertexIds(col));
	
		int minIterations = lb;
		int maxIterations = ub;
		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
		IterativeDataSet<Tuple2<Long, Long>> getInitialWorkset = verticesWorkset.iterate(minIterations);
		
		DataSet<Tuple2<Long, Long>> initialResults = getInitialWorkset
				.join(graph.getEdges(), strategy)
				.where(1)
				.equalTo(1)
				.with(new FilterEdgesByLabel(label));
		
		DataSet<Tuple2<Long, Long>> initialWorkset = getInitialWorkset
				.closeWith(initialResults)
				.map(new GetStartingVertexIds())
				.groupBy(0, 1)
				.reduceGroup(new DuplicatesReduction());
		
		if (ub == lb) {
			DataSet<ArrayList<Long>> results = this.paths
					.join(initialWorkset)
					.where(verticesSelector)
					.equalTo(0)
					.with(new UpdateVertexAndEdgeIds());
			this.paths = results;
			return results;
		}
		else {
			IterativeDataSet<Tuple2<Long, Long>> iteration = initialWorkset.iterate(maxIterations);
		
			DataSet<Tuple2<Long,Long>> nextResults = iteration
					.join(graph.getEdges(), strategy)
					.where(1)
					.equalTo(1)
					.with(new FilterEdgesByLabel(label))
					.union(iteration)
					.groupBy(0, 1)
					.reduceGroup(new DuplicatesReduction())
					.withForwardedFields("0;1");
		
			//Using this coGroup to quickly detect whether new vertex pairs are added, if not, terminate the iterations
			DataSet<Tuple2<Long,Long>> newResults = iteration
					.coGroup(nextResults)
					.where(0)
					.equalTo(0)
					.with(new GetNewResults())
					.withForwardedFieldsFirst("0")
					.withForwardedFieldsSecond("0");

			DataSet<Tuple2<Long, Long>> mergedResults = iteration.closeWith(nextResults, newResults);
		
			DataSet<ArrayList<Long>> results = this.paths
					.join(mergedResults)
					.where(verticesSelector)
					.equalTo(0)
					.with(new UpdateVertexAndEdgeIds());
		
			this.paths = results;
			return results;
		}
	}

	//define upper bound and lower bound of the number of edges traversed
	public DataSet<ArrayList<Long>> matchWithUpperBound(int col, int ub, String label, JoinHint strategy) throws Exception {
		//Initial WorkSet DataSet consisting of vertex-pair IDs for Delta Iteration. Each field of Tuple2<Long, Long> stores two same IDs since these two are starting vertices
		DataSet<Tuple2<Long, Long>> verticesWorkset = this.paths
				.map(new ExtractVertexIds(col));
		
		int maxIterations = ub;
		IterativeDataSet<Tuple2<Long, Long>> iteration = verticesWorkset.iterate(maxIterations);
		
		DataSet<Tuple2<Long,Long>> nextResults = iteration
				.join(graph.getEdges(), strategy)
				.where(1)
				.equalTo(1)
				.with(new FilterEdgesByLabel(label))
				.union(iteration)
				.groupBy(0, 1)
				.reduceGroup(new DuplicatesReduction())
			    .withForwardedFields("0;1");

		DataSet<Tuple2<Long,Long>> newResults = iteration
				.coGroup(nextResults)
				.where(0)
				.equalTo(0)
				.with(new GetNewResults())
				.withForwardedFieldsFirst("0")
				.withForwardedFieldsSecond("0");

		DataSet<Tuple2<Long, Long>> mergedResults = iteration.closeWith(nextResults, newResults);
		
		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col); 
		DataSet<ArrayList<Long>> results = this.paths
				.join(mergedResults)
				.where(verticesSelector)
				.equalTo(0)
				.with(new UpdateVertexAndEdgeIds());
		
		this.paths = results;
		return results;
	}

	//define lower bound of the number of edges traversed
	public DataSet<ArrayList<Long>> matchWithLowerBound(int col, int lb, String label, JoinHint strategy) throws Exception {
		
		DataSet<Tuple2<Long, Long>> verticesWorkset = this.paths
				.map(new ExtractVertexIds(col));
		int minIterations = lb;
		int maxIterations = 1000;
		
		IterativeDataSet<Tuple2<Long, Long>> getInitialWorkset = verticesWorkset.iterate(minIterations);
		
		DataSet<Tuple2<Long, Long>> initialResults = getInitialWorkset
				.join(graph.getEdges(), strategy)
				.where(1)
				.equalTo(1)
				.with(new FilterEdgesByLabel(label));
		
		DataSet<Tuple2<Long, Long>> initialWorkset = getInitialWorkset
				.closeWith(initialResults)
				.map(new GetStartingVertexIds())
				.groupBy(0, 1)
				.reduceGroup(new DuplicatesReduction());

		IterativeDataSet<Tuple2<Long, Long>> iteration = initialWorkset.iterate(maxIterations);
		
		DataSet<Tuple2<Long,Long>> nextResults = iteration
				.join(graph.getEdges(), strategy)
				.where(1)
				.equalTo(1)
				.with(new FilterEdgesByLabel(label))
				.union(iteration)
				.groupBy(0, 1)
				.reduceGroup(new DuplicatesReduction())
			    .withForwardedFields("0;1");
		
		//Using this coGroup to quickly detect whether new vertex pairs are added, if not, terminate the iterations
		DataSet<Tuple2<Long,Long>> newResults = iteration
				.coGroup(nextResults)
				.where(0)
				.equalTo(0)
				.with(new GetNewResults())
				.withForwardedFieldsFirst("0")
				.withForwardedFieldsSecond("0");

		DataSet<Tuple2<Long, Long>> mergedResults = iteration.closeWith(nextResults, newResults);
		
		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col); 
		DataSet<ArrayList<Long>> results = this.paths
				.join(mergedResults)
				.where(verticesSelector)
				.equalTo(0)
				.with(new UpdateVertexAndEdgeIds());
		
		this.paths = results;
		return results;
		
	}

	//Do not define any bounds
	public DataSet<ArrayList<Long>> matchWithoutBounds(int col, String label, JoinHint strategy) throws Exception {
		//Initial WorkSet DataSet consisting of vertex-pair IDs for Delta Iteration Each field of Tuple2<Long, Long> stores two same IDs since these two are starting vertices
		DataSet<Tuple2<Long, Long>> verticesWorkset = this.paths
				.map(new ExtractVertexIds(col));
	
		int maxIterations = 100000;
	
		IterativeDataSet<Tuple2<Long, Long>> iteration = verticesWorkset.iterate(maxIterations);
		
		DataSet<Tuple2<Long,Long>> nextResults = iteration
				.join(graph.getEdges(), strategy)
				.where(1)
				.equalTo(1)
				.with(new FilterEdgesByLabel(label))
				.union(iteration)
				.groupBy(0, 1)
				.reduceGroup(new DuplicatesReduction())
			    .withForwardedFields("0;1");
		
		//Using this coGroup to quickly detect whether new vertex pairs are added, if not, terminate the iterations
		DataSet<Tuple2<Long,Long>> newResults = iteration
				.coGroup(nextResults)
				.where(0)
				.equalTo(0)
				.with(new GetNewResults())
				.withForwardedFieldsFirst("0")
				.withForwardedFieldsSecond("0");

		DataSet<Tuple2<Long, Long>> mergedResults = iteration.closeWith(nextResults, newResults);
		
		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col); 
		DataSet<ArrayList<Long>> results = this.paths
				.join(mergedResults)
				.where(verticesSelector)
				.equalTo(0)
				.with(new UpdateVertexAndEdgeIds());
		
		this.paths = results;
		return results;
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
	
	private static class GetStartingVertexIds implements MapFunction<Tuple2<Long, Long>, 
			Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> map(Tuple2<Long, Long> vertexIds)
				throws Exception {
			
			return new Tuple2<Long, Long>(vertexIds.f0, vertexIds.f1);
		}
		
	}
	
	private static class DuplicatesReduction implements GroupReduceFunction<Tuple2<Long, Long>, 
			Tuple2<Long, Long>> {
		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> vertexIds, Collector<Tuple2<Long, Long>> out){
			out.collect(vertexIds.iterator().next());
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
	
	private static class UpdateVertexAndEdgeIds implements FlatJoinFunction<ArrayList<Long>,
			Tuple2<Long, Long>, ArrayList<Long>> {
		@Override
		public void join(
				ArrayList<Long> vertexAndEdgeIds,
				Tuple2<Long, Long> vertexIds,
				Collector<ArrayList<Long>> updateIdsList) throws Exception {
			//vertexAndEdgeIds.add(Long.MAX_VALUE);
			vertexAndEdgeIds.add(vertexIds.f1);
			updateIdsList.collect(vertexAndEdgeIds);
		}
	}
	

}
