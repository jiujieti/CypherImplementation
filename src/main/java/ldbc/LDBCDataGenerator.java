package ldbc;


import org.apache.flink.api.java.ExecutionEnvironment;

public class LDBCDataGenerator {
	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	
		//String dir = args[0];
		//String dir = "C:/Datasets/ldbc_dataset_small/";
		String dir = "C:/Users/s146508/Desktop/Ubuntu/10Person/";
		LDBCToGraphDataModel ldbcGraph = new LDBCToGraphDataModel(dir, env);
		ldbcGraph.getGraph();
	
	}
}
