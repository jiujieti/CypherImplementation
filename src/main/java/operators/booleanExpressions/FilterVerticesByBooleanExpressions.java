package operators.booleanExpressions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import operators.datastructures.VertexExtended;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class FilterVerticesByBooleanExpressions implements FlatJoinFunction<ArrayList<Long>, VertexExtended<Long, HashSet<String>,
HashMap<String, String>>, ArrayList<Long>>{

	private FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> filterVertices;
	
	
	public FilterVerticesByBooleanExpressions(FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> filterVertices) {
		this.filterVertices = filterVertices;
	}

	@Override
	public void join(
			ArrayList<Long> vertexId,
			VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex,
			Collector<ArrayList<Long>> selectedVertexId) throws Exception {
		if(this.filterVertices.filter(vertex) == true)
			selectedVertexId.collect(vertexId);
	}
	
}