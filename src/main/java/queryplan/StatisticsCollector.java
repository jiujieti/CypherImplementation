package queryplan;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
/**
 * Collect statistic information from graph database
 * Information includes the total number of edges and vertices,
 * vertex and edge labels and their corresponding proportion of the total number
 * */
@SuppressWarnings("serial")
public class StatisticsCollector {
	public static void main(String[] args) throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		String srcDir = args[0];
		String tarDir = args[1];
		//String srcDir = "C:/Users/s146508/Desktop/ubuntu/5kPerson/";
		//String srcDir = "C:/Users/s146508/Desktop/ubuntu/a/";
		//String tarDir = srcDir;
		StatisticsCollector k = new StatisticsCollector(env, srcDir, tarDir);
		k.getEdgesStats();
		k.getVerticesStats();
	}	
	ExecutionEnvironment env;
	String srcDir;
	String tarDir;
	
	public StatisticsCollector(ExecutionEnvironment e, String s, String t) {
		env = e;
		srcDir = s;
		tarDir = t;
	}
	
	public DataSet<Tuple3<String, Long, Double>>  getVerticesStats () throws Exception {
		
		DataSet<Tuple3<Long, String, String>> verticesFromFile = env.readCsvFile(srcDir + "vertices.csv")
			.fieldDelimiter("|")
			.types(Long.class, String.class, String.class);
		
		DataSet<Tuple2<String, Long>> vertexLabels = verticesFromFile.flatMap(new ExtractVertexLabels()).groupBy(0).sum(1); 
		
		long vertexNum = verticesFromFile.count();
		
		DataSet<Tuple2<String, Long>> vertexNumber = env.fromElements("vertices" + " " + String.valueOf(vertexNum)).map(new StringToTuple());
		
		DataSet<Tuple3<String, Long, Double>> vertices = vertexLabels.union(vertexNumber)
				.map(new ProportionComputation(vertexNum))
				.sortPartition(1, Order.DESCENDING);
		
		vertices.writeAsCsv(tarDir + "vertices", "\n", "|");
		
		env.execute();
		return vertices;
	}
	
	public DataSet<Tuple3<String, Long, Double>> getEdgesStats () throws Exception {
		
		DataSet<Tuple5<Long, Long, Long, String, String>> edgesFromFile = env.readCsvFile(srcDir + "edges.csv")
				.fieldDelimiter("|")
				.types(Long.class, Long.class, Long.class, String.class, String.class);
		
		DataSet<Tuple2<String, Long>> edgeLabels = edgesFromFile.map(new ExtractEdgeLabels()).groupBy(0).sum(1); 
		
		long edgeNum = edgesFromFile.count();
		
		DataSet<Tuple2<String, Long>> edgeNumber = env.fromElements("edges" + " " + String.valueOf(edgeNum)).map(new StringToTuple());
		
		DataSet<Tuple3<String, Long, Double>> edges = edgeLabels.union(edgeNumber)
				.map(new ProportionComputation(edgeNum))
				.sortPartition(1, Order.DESCENDING);
		

		edges.writeAsCsv(tarDir + "edges", "\n", "|");
		env.execute();
		return edges;

	}
	
	//compute the distribution in the form of (v) -> [e]
	public void getVEDistStats (DataSet<Tuple3<String, Long, Double>> vs, DataSet<Tuple3<String, Long, Double>> es) {	
		
		DataSet<Tuple3<Long, String, String>> verticesFromFile = env.readCsvFile(srcDir + "vertices.csv")
				.fieldDelimiter("|")
				.types(Long.class, String.class, String.class);
		
		DataSet<Tuple5<Long, Long, Long, String, String>> edgesFromFile = env.readCsvFile(srcDir + "edges.csv")
				.fieldDelimiter("|")
				.types(Long.class, Long.class, Long.class, String.class, String.class);
		
		//Tuple4<v-label, e-label, num, num/v_num>	
		//ArrayList<Tuple3<String, Long, Double>> vsl = (ArrayList<Tuple3<String, Long, Double>>) vs.collect();
		//ArrayList<Tuple3<String, Long, Double>> esl = (ArrayList<Tuple3<String, Long, Double>>) es.collect();
		
		//DataSet<Tuple4<String, String, Long, Double>> srcVertexDist = vs.
		
	
	}
	
	public void getEVDistStats () {
		//compute the distribution in the form of [e] -> (v)
		
	}
	
	private static class ExtractVertexLabels implements FlatMapFunction<Tuple3<Long, String, String>, Tuple2<String, Long>>{

		@Override
		public void flatMap(Tuple3<Long, String, String> vertex, Collector<Tuple2<String, Long>> labels)
				throws Exception {
			String[] ls = vertex.f1.substring(1, vertex.f1.length()-1).split(",");
			for(String label : ls) {
				labels.collect(new Tuple2<String, Long>(label, 1L));
			}
		}
	}

	private static class StringToTuple implements MapFunction<String, Tuple2<String, Long>> {

		@Override
		public Tuple2<String, Long> map(String sum) throws Exception {
			String[] total = sum.split(" ");
			return new Tuple2<String, Long>(total[0], Long.valueOf(total[1]));
		}
	}
	
	private static class ProportionComputation implements MapFunction<Tuple2<String, Long>, Tuple3<String, Long, Double>> {

		private long totalNum;
		public ProportionComputation(long totalNum) { this.totalNum = totalNum; }
		
		@Override
		public Tuple3<String, Long, Double> map(Tuple2<String, Long> vertex)
				throws Exception {
			return new Tuple3<String, Long, Double>(vertex.f0, vertex.f1, (double)vertex.f1 * 1.0/totalNum);
		}
	}
	
	private static class ExtractEdgeLabels implements MapFunction<Tuple5<Long, Long, Long, String, String>, Tuple2<String, Long>> {

		@Override
		public Tuple2<String, Long> map(
				Tuple5<Long, Long, Long, String, String> edge)
				throws Exception {
			return new Tuple2<String, Long>(edge.f3, 1L);
		}
	}
}
