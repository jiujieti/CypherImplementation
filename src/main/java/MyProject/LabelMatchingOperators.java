package MyProject;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import MyProject.DataStructures.GraphExtended;
import MyProject.DataStructures.EdgeExtended;
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
	
	public DataSet<Tuple2<String, Long>> matchWithoutBounds(final int col, String label) {
		
		DataSet<Tuple1<Long>> initialDeltaSet = this.vertexAndEdgeIds.map(
				new MapFunction<ArrayList<Tuple2<String, Long>>, Tuple1<Long>>(){

					public Tuple1<Long> map(
							ArrayList<Tuple2<String, Long>> value)
							throws Exception {
						return new Tuple1<Long>(value.get(col).f1);
					}
			
		});
		
		DataSet<Tuple2<String, Long>> initialSolutionSet = this.vertexAndEdgeIds.map(
				new MapFunction<ArrayList<Tuple2<String, Long>>, Tuple2<String, Long>>(){

					public Tuple2<String, Long> map(
							ArrayList<Tuple2<String, Long>> value)
							throws Exception {
						return value.get(col);
					}
			
		});
		int maxIterations = 1;
		
		/**/
		DeltaIteration<Tuple2<String, Long>, Tuple1<Long>> iteration = 
			initialSolutionSet
				.iterateDelta(initialDeltaSet, maxIterations, 1);
		
		/*new work set??*/
		DataSet<Tuple1<Long>> nextWorkset = iteration.getWorkset()
					.join(graph.getEdges())
					.where(0)
					.equalTo(1)
					.with(new FilterEdgesByLabel(label));
		
		DataSet<Tuple2<String, Long>> deltas = nextWorkset
					.join(iteration.getSolutionSet())
					.where(0)
					.equalTo(1)
					.with(new AddTargetVertices());

		return iteration.closeWith(deltas, nextWorkset);
		
	
	}

	
	private static class FilterEdgesByLabel implements FlatJoinFunction<Tuple1<Long>, 
		EdgeExtended<String, Long, String, HashMap<String, String>>, Tuple1<Long>>{

		private String lab = "";
		FilterEdgesByLabel(String label) { this.lab = label; }
		@Override
		public void join(
				Tuple1<Long> vertexId,
				EdgeExtended<String, Long, String, HashMap<String, String>> edge,
				Collector<Tuple1<Long>> out) throws Exception {
			if(edge.f3.equals(lab))
				out.collect(new Tuple1<Long>(edge.f2));			
		}
	}
	
	private static class KeySelectorForVertices implements KeySelector<ArrayList<Tuple2<String, Long>>, Long> {

		private int col = 0;
		KeySelectorForVertices(int column) {this.col = column;}
		
		@Override
		public Long getKey(ArrayList<Tuple2<String, Long>> vertexAndEdgeIds)
				throws Exception {	
			return vertexAndEdgeIds.get(col).f1;
		}
	}
	
	private static class AddTargetVertices implements JoinFunction<Tuple1<Long>, 
		Tuple2<String, Long>, Tuple2<String, Long>> {

		@Override
		public Tuple2<String, Long> join(Tuple1<Long> tgtVertexId,
				Tuple2<String, Long> vertexAndEdgeIds) 
				throws Exception {
			
			return new Tuple2<String, Long>(null, tgtVertexId.f0);
		}
	}
}
