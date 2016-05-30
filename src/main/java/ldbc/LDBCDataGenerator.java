package ldbc;

import java.util.HashMap;
import java.util.HashSet;

import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;
import operators.datastructures.VertexExtended;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class LDBCDataGenerator {
	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	
		String dir = "C:/Users/s146508/Desktop/ubuntu/35kPerson/";
		LDBCToGraphDataModel ldbcGraph = new LDBCToGraphDataModel(dir, env);
		ldbcGraph.getGraph();
	
	}
}
