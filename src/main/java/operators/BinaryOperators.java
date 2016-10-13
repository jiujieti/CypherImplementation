package operators;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;
/*
*
* All binary operators are implemented here.
* 1. Join operator. Each join operation joins two paths on a common vertex. The difference between joinOnBeforeVertices and joinOnAfterVertices is about the concatenation order of this two paths;
* 2. Union operator.
* 3. Intersection operator.
*
* */
@SuppressWarnings("serial")
public class BinaryOperators {
	
	//Each list contains the vertex IDs and edge IDs of a selected path so far 
	private DataSet<ArrayList<Long>> pathsLeft;
	private DataSet<ArrayList<Long>> pathsRight;
	
	
	//Get the input graph, current columnNumber and the vertex and edges IDs
	public BinaryOperators(
			DataSet<ArrayList<Long>> pathsLeft,
			DataSet<ArrayList<Long>> pathsRight) {
		this.pathsLeft = pathsLeft;
		this.pathsRight = pathsRight;
	}
	
	//Join on after vertices
	public DataSet<ArrayList<Long>> joinOnAfterVertices(int firstCol, int secondCol) {
		KeySelectorForColumns SelectorFisrt = new KeySelectorForColumns(firstCol);
		KeySelectorForColumns SelectorSecond = new KeySelectorForColumns(secondCol);
		
		DataSet<ArrayList<Long>> joinedResults = this.pathsLeft
				.join(this.pathsRight)
				.where(SelectorFisrt)
				.equalTo(SelectorSecond)
				.with(new JoinOnAfterVertices(secondCol));
		return joinedResults;
	}
	
	private static class JoinOnAfterVertices implements JoinFunction<ArrayList<Long>,
	ArrayList<Long>, ArrayList<Long>>{

		private int col;
		
		public JoinOnAfterVertices(int secondCol) {this.col = secondCol;}
		@Override
		public ArrayList<Long> join(ArrayList<Long> leftVertices,
				ArrayList<Long> rightVertices) throws Exception {
			rightVertices.remove(this.col);
			leftVertices.addAll(rightVertices);
			return leftVertices;
		}	
	}

	
	//Join on left vertices
	public DataSet<ArrayList<Long>> joinOnBeforeVertices(int firstCol, int secondCol) {
		KeySelectorForColumns SelectorFisrt = new KeySelectorForColumns(firstCol);
		KeySelectorForColumns SelectorSecond = new KeySelectorForColumns(secondCol);
		
		DataSet<ArrayList<Long>> joinedResults = this.pathsLeft
				.join(this.pathsRight)
				.where(SelectorFisrt)
				.equalTo(SelectorSecond)
				.with(new JoinOnBeforeVertices(firstCol));
		return joinedResults;
	}
	
	private static class JoinOnBeforeVertices implements JoinFunction<ArrayList<Long>,
	ArrayList<Long>, ArrayList<Long>>{
		
		private int col;
		
		public JoinOnBeforeVertices(int firstCol) {this.col = firstCol;}
		@Override
		public ArrayList<Long> join(ArrayList<Long> leftPaths,
				ArrayList<Long> rightPaths) throws Exception {
			leftPaths.remove(this.col);
			rightPaths.addAll(leftPaths);
			return rightPaths;
		}
	}
	
	//Union
	public DataSet<ArrayList<Long>> union(){
		DataSet<ArrayList<Long>> unitedResults = this.pathsLeft
				.union(this.pathsRight)
				.distinct();
		return unitedResults;
	}
	
	//Intersection
	public DataSet<ArrayList<Long>> intersection() {
		DataSet<ArrayList<Long>> intersectedResults = this.pathsLeft
				.join(this.pathsRight)
				.where(0)
				.equalTo(0)
				.with(new IntersectionResultsMerge());
		return intersectedResults;
	}
	
	private static class IntersectionResultsMerge implements FlatJoinFunction<ArrayList<Long>, ArrayList<Long>, ArrayList<Long>> {
		@Override
		public void join(ArrayList<Long> leftPath, ArrayList<Long> rightPath, Collector<ArrayList<Long>> intersectedPath) throws Exception {
			if(leftPath.equals(rightPath)) intersectedPath.collect(leftPath);
		}
	}
	
	
}
