package ldbc.join;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class EdgeIdReplacerRightThreeElements implements JoinFunction<Tuple3<Long, Long, String>, Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>, 
	Tuple3<Long, Long, String>>{

	@Override
	public Tuple3<Long, Long, String> join(Tuple3<Long, Long, String> edge,
			Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> vertex)
					throws Exception {
		return new Tuple3<Long, Long, String>(edge.f0, vertex.f0, edge.f2);
	}
}

