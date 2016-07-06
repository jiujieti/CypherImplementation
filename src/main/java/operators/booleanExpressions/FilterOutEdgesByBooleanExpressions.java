package operators.booleanExpressions;

import java.util.ArrayList;
import java.util.HashMap;

import operators.datastructures.EdgeExtended;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class FilterOutEdgesByBooleanExpressions implements FlatJoinFunction<ArrayList<Long>, EdgeExtended<Long, Long, String, HashMap<String, String>>, ArrayList<Long>>{

	private FilterFunction<EdgeExtended<Long, Long, String, HashMap<String, String>>> filterEdges;
	
	
	public FilterOutEdgesByBooleanExpressions(FilterFunction<EdgeExtended<Long, Long, String, HashMap<String, String>>> filterEdges) {
		this.filterEdges = filterEdges;
	}

	@Override
	public void join(
			ArrayList<Long> edgeId,
			EdgeExtended<Long, Long, String, HashMap<String, String>> edge,
			Collector<ArrayList<Long>> selectedVertexId) throws Exception {
		if(this.filterEdges.filter(edge) == true) {
			edgeId.add(edge.f0);
			edgeId.add(edge.f2);
			selectedVertexId.collect(edgeId);
		}
	}
	
}

