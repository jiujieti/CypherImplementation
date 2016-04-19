package ldbc.join;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

@SuppressWarnings("serial")
public class EdgeIdReplacerLeft implements JoinFunction<Tuple2<Long, Long>, Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>, Tuple2<Long, Long>>{
	
	@Override
	public Tuple2<Long, Long> join(Tuple2<Long, Long> edge,
			Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> vertex)
					throws Exception {
		return new Tuple2<Long, Long>(vertex.f0, edge.f1);
	}
}
