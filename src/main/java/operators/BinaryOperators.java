package operators;

import java.util.ArrayList;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;

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
	public DataSet<ArrayList<Long>> JoinOnAfterVertices(int firstCol, int secondCol) {
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
	public DataSet<ArrayList<Long>> JoinOnBeforeVertices(int firstCol, int secondCol) {
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
	
	private static class IntersectionResultsMerge implements JoinFunction<ArrayList<Long>, ArrayList<Long>, ArrayList<Long>> {
		@Override
		public ArrayList<Long> join(ArrayList<Long> leftPaths, ArrayList<Long> rightPaths) throws Exception {
			return leftPaths;
		}
	}
	
	
}
