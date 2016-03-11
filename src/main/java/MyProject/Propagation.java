package MyProject;

import java.util.HashMap;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Edge;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;

public class Propagation {
	public DataSet<Edge<Long, HashMap<String, String>>> resEdgesDataSet(
			DataSet<Edge<Long, HashMap<String, String>>> inputEdgeDataSet, 
			Graph<Long, HashMap<String, String>, HashMap<String, String>> inputGraph){
		DataSet<Edge<Long, HashMap<String, String>>> resEdges = inputEdgeDataSet
				.join(inputGraph.getEdges())
				.where(1)
				.equalTo(0)
				.with(new FlatJoinFunction<Edge<Long, HashMap<String, String>>, 
						Edge<Long, HashMap<String, String>>,
						Edge<Long, HashMap<String, String>>>() {
					public void join(Edge<Long, HashMap<String, String>> e1,
							Edge<Long, HashMap<String, String>> e2, Collector<Edge<Long, HashMap<String, String>>> out) {
						out.collect(e2);
					}
				});
		
		return inputEdgeDataSet;
	}
}
