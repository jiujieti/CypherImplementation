package MyProject;

import java.util.HashMap;
import java.util.ArrayList;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple1;
import MyProject.DataStructures.GraphExtended;


public class ScanOperators {
	private final GraphExtended<Long, ArrayList<String>, HashMap<String, String>, String,
	  String, HashMap<String, String>> graph;
		
	/*get the input graph*/
	public ScanOperators(GraphExtended<Long, ArrayList<String>, HashMap<String, String>, String,
			  String, HashMap<String, String>> g) {
		this.graph = g;
	}
	
	/* get the initial <edge, vertex> pair of the graph
	 * the node retrieved here should contain no incoming edges
	 * hence the edge ids are set to "e0" */
	public DataSet<ArrayList<Tuple2<String, Long>>> 
		getInitialVertices() {
		DataSet<ArrayList<Tuple2<String, Long>>> vertexAndEdges = graph
			.getAllVertexIds().map(new InitialVerticesToLists());
		return vertexAndEdges;
	}
	
	private static class InitialVerticesToLists implements MapFunction<Tuple1<Long>,
	ArrayList<Tuple2<String, Long>>> {
		
		private static final long serialVersionUID = 1L;
		
		@Override
		public ArrayList<Tuple2<String, Long>> map(
				Tuple1<Long> vertex) throws Exception {
			Tuple2<String, Long> edgeAndVertex = new Tuple2<>("e0", vertex.f0);
			ArrayList<Tuple2<String, Long>> row = new ArrayList<>();
			row.add(edgeAndVertex);
			return row;
		}
	  }
}
	
	

