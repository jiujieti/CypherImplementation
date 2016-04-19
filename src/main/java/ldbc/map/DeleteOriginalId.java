package ldbc.map;

import java.util.HashMap;
import java.util.HashSet;

import operators.datastructures.VertexExtended;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;



@SuppressWarnings("serial")
public class DeleteOriginalId implements MapFunction<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>, VertexExtended<Long, HashSet<String>, HashMap<String, String>>> {

	@Override
	public VertexExtended<Long, HashSet<String>, HashMap<String, String>> map(
			Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> vertex)
			throws Exception {
		return new VertexExtended<Long, HashSet<String>, HashMap<String, String>>(vertex.f0, vertex.f1, vertex.f2);
	}
	
}
