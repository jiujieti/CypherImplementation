package MyProject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import MyProject.DataStructures.GraphExtended;
import MyProject.DataStructures.EdgeExtended;
import MyProject.KeySelectorForVertices;
/* Using for loop to do the iterations now to collect those vertex IDs
 * Some flaws are still existing here:
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
			  String, HashMap<String, String>> graph,
			  DataSet<ArrayList<Tuple2<String, Long>>> verticesAndEdges) {
		this.graph = graph;
		this.vertexAndEdgeIds = verticesAndEdges;
	}
	
	public DataSet<ArrayList<Tuple2<String, Long>>> matchWithBounds(int col, int lb, int ub, String label) throws Exception {

		DataSet<Tuple2<Long, Long>> verticesWorkset = this.vertexAndEdgeIds
				.map(new ExtractVertexIds(col));
	
		int minIterations = lb;
		int maxIterations = ub;
		KeySelectorForVertices verticesSelector = new KeySelectorForVertices(col);
		IterativeDataSet<Tuple2<Long, Long>> getInitialWorkset = verticesWorkset.iterate(minIterations);
		
		DataSet<Tuple2<Long, Long>> initialResults = getInitialWorkset
				.join(graph.getEdges())
				.where(1)
				.equalTo(1)
				.with(new FilterEdgesByLabel(label));
		
		DataSet<Tuple2<Long, Long>> initialWorkset = getInitialWorkset
				.closeWith(initialResults)
				.map(new GetStartingVertexIds())
				.groupBy(0, 1)
				.reduceGroup(new DuplicatesReduction());
		
		if (ub == lb) {
			DataSet<ArrayList<Tuple2<String, Long>>> results = this.vertexAndEdgeIds
					.groupBy(verticesSelector)
					.getDataSet()
					.join(initialWorkset)
					.where(verticesSelector)
					.equalTo(0)
					.with(new UpdateVertexAndEdgeIds());
			this.vertexAndEdgeIds = results;
			return results;
		}
		else {
			IterativeDataSet<Tuple2<Long, Long>> iteration = initialWorkset.iterate(maxIterations);
		
			DataSet<Tuple2<Long,Long>> nextResults = iteration
					.join(graph.getEdges())
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
		
			DataSet<ArrayList<Tuple2<String, Long>>> results = this.vertexAndEdgeIds
					.groupBy(verticesSelector)
					.getDataSet()
					.join(mergedResults)
					.where(verticesSelector)
					.equalTo(0)
					.with(new UpdateVertexAndEdgeIds());
		
			this.vertexAndEdgeIds = results;
			return results;
		}
	} 
	
	public DataSet<ArrayList<Tuple2<String, Long>>> matchWithUpperBound(int col, int ub, String label) throws Exception {
		//Initial WorkSet DataSet consisting of vertex-pair IDs for Delta Iteration. Each field of Tuple2<Long, Long> stores two same IDs since these two are starting vertices
		DataSet<Tuple2<Long, Long>> verticesWorkset = this.vertexAndEdgeIds
				.map(new ExtractVertexIds(col));
		
		int maxIterations = ub;
		IterativeDataSet<Tuple2<Long, Long>> iteration = verticesWorkset.iterate(maxIterations);
		
		DataSet<Tuple2<Long,Long>> nextResults = iteration
				.join(graph.getEdges())
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
		
		KeySelectorForVertices verticesSelector = new KeySelectorForVertices(col); 
		DataSet<ArrayList<Tuple2<String, Long>>> results = this.vertexAndEdgeIds
				.groupBy(verticesSelector)
				.getDataSet()
				.join(mergedResults)
				.where(verticesSelector)
				.equalTo(0)
				.with(new UpdateVertexAndEdgeIds());
		
		this.vertexAndEdgeIds = results;
		return results;
	} 
	
	public DataSet<ArrayList<Tuple2<String, Long>>> matchWithLowerBound(int col, int lb, String label) throws Exception {
		
		DataSet<Tuple2<Long, Long>> verticesWorkset = this.vertexAndEdgeIds
				.map(new ExtractVertexIds(col));
		int minIterations = lb;
		int maxIterations = 1000;
		
		IterativeDataSet<Tuple2<Long, Long>> getInitialWorkset = verticesWorkset.iterate(minIterations);
		
		DataSet<Tuple2<Long, Long>> initialResults = getInitialWorkset
				.join(graph.getEdges())
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
				.join(graph.getEdges())
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
		
		KeySelectorForVertices verticesSelector = new KeySelectorForVertices(col); 
		DataSet<ArrayList<Tuple2<String, Long>>> results = this.vertexAndEdgeIds
				.groupBy(verticesSelector)
				.getDataSet()
				.join(mergedResults)
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
	
		int maxIterations = 1000;
	
		IterativeDataSet<Tuple2<Long, Long>> iteration = verticesWorkset.iterate(maxIterations);
		
		DataSet<Tuple2<Long,Long>> nextResults = iteration
				.join(graph.getEdges())
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
		
		KeySelectorForVertices verticesSelector = new KeySelectorForVertices(col); 
		DataSet<ArrayList<Tuple2<String, Long>>> results = this.vertexAndEdgeIds
				.groupBy(verticesSelector)
				.getDataSet()
				.join(mergedResults)
				.where(verticesSelector)
				.equalTo(0)
				.with(new UpdateVertexAndEdgeIds());
		
		this.vertexAndEdgeIds = results;
		return results;
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
	
	private static class GetStartingVertexIds implements MapFunction<Tuple2<Long, Long>, 
			Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> map(Tuple2<Long, Long> vertexIds)
				throws Exception {
			
			return new Tuple2<Long, Long>(vertexIds.f1, vertexIds.f1);
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
			Set<Tuple2<Long,Long>> prevResults = new HashSet<Tuple2<Long,Long>>();
			for (Tuple2<Long,Long> prev : originalVertexIds) {
				prevResults.add(prev);
			}
			for (Tuple2<Long,Long> next: newVertexIds) {
				if (!prevResults.contains(next)) {
					results.collect(next);
				}
			}
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
