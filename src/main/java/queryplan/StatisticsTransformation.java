package queryplan;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;

/*
* Read the files storing statistical information and transform them to the vertices and edges defined in the package operators.datastructure
* */

public class StatisticsTransformation {
	String dir;
	ExecutionEnvironment env;
	
	public StatisticsTransformation(String d, ExecutionEnvironment e) {
		dir = d;
		env = e;
	}
	
	public HashMap<String, Tuple2<Long, Double>> getVerticesStatistics() throws Exception {
		DataSet<Tuple3<String, Long, Double>> vertices = env.readCsvFile(dir + "vertices")
				.fieldDelimiter("|")
				.types(String.class, Long.class, Double.class);
		
		ArrayList<Tuple3<String, Long, Double>> vs = (ArrayList<Tuple3<String, Long, Double>>) vertices.collect();
		HashMap<String, Tuple2<Long, Double>> vsi = new HashMap<>();
		for(Tuple3<String, Long, Double> v: vs) {
			vsi.put(v.f0, new Tuple2<Long, Double>(v.f1, v.f2));
		}
		return vsi;
	}


	public HashMap<String, Tuple2<Long, Double>> getEdgesStatistics() throws Exception {
		DataSet<Tuple3<String, Long, Double>> edges = env.readCsvFile(dir + "edges")
				.fieldDelimiter("|")
				.types(String.class, Long.class, Double.class);

		ArrayList<Tuple3<String, Long, Double>> es = (ArrayList<Tuple3<String, Long, Double>>) edges.collect();
		HashMap<String, Tuple2<Long, Double>> esi = new HashMap<>();
		for(Tuple3<String, Long, Double> e: es) {
			esi.put(e.f0, new Tuple2<Long, Double>(e.f1, e.f2));
		}
		return esi;
	}
}
