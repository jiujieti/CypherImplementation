package gmark;

import org.apache.flink.api.java.ExecutionEnvironment;

public class GMarkDataGenerator {
	public static void main (String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		String dir = args[0];
		//String dir = "C:/Users/s146508/Desktop/";
		
		GMarkToGraphDataModel gmarkGraph = new GMarkToGraphDataModel(dir, env);
		gmarkGraph.getGraph();
		
	}
}
