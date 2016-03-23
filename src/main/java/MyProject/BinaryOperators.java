package MyProject;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

import MyProject.DataStructures.GraphExtended;
@SuppressWarnings("serial")
public class BinaryOperators {
	//Input graph
	
		
	//Each list contains the vertex IDs and edge IDs of a selected path so far 
	private DataSet<ArrayList<Tuple2<String, Long>>> vertexAndEdgeIdsLeft;
	private DataSet<ArrayList<Tuple2<String, Long>>> vertexAndEdgeIdsRight;
	
	
	//Get the input graph, current columnNumber and the vertex and edges IDs
	public BinaryOperators(GraphExtended<Long, ArrayList<String>, HashMap<String, String>, String,
			String, HashMap<String, String>> graph,
			DataSet<ArrayList<Tuple2<String, Long>>> verticesAndEdgesLeft,
			DataSet<ArrayList<Tuple2<String, Long>>> verticesAndEdgesRight) {
		this.vertexAndEdgeIdsLeft = verticesAndEdgesLeft;
		this.vertexAndEdgeIdsRight = verticesAndEdgesRight;
	}
	
	//Join on vertices
	public DataSet<ArrayList<Tuple2<String, Long>>> JoinOnVertices(int firstCol, int secondCol) {
		KeySelectorForVertices verticesSelectorFisrt = new KeySelectorForVertices(firstCol);
		KeySelectorForVertices verticesSelectorSecond = new KeySelectorForVertices(secondCol);
		
		DataSet<ArrayList<Tuple2<String, Long>>> joinedResults = this.vertexAndEdgeIdsLeft
				.groupBy(verticesSelectorFisrt)
				.getDataSet()
				.join(this.vertexAndEdgeIdsRight.groupBy(verticesSelectorSecond).getDataSet())
				.where(verticesSelectorFisrt)
				.equalTo(verticesSelectorSecond)
				.with(new JoinOnVertices());
		return joinedResults;
	}
	
	//Union
	public DataSet<ArrayList<Tuple2<String, Long>>> Union(){
		DataSet<ArrayList<Tuple2<String, Long>>> unitedResults = this.vertexAndEdgeIdsLeft
				.union(this.vertexAndEdgeIdsRight);
		return unitedResults;
	}
	
	
	private static class JoinOnVertices implements JoinFunction<ArrayList<Tuple2<String, Long>>,
			ArrayList<Tuple2<String, Long>>, ArrayList<Tuple2<String, Long>>>{

		@Override
		public ArrayList<Tuple2<String, Long>> join(
				ArrayList<Tuple2<String, Long>> leftVertices,
				ArrayList<Tuple2<String, Long>> rightVertices) throws Exception {
			leftVertices.addAll(rightVertices);
			return leftVertices;
		}
		
	}
}
