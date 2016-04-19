package ldbc.map;

import java.util.HashMap;

import operators.datastructures.EdgeExtended;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple2;

@SuppressWarnings("serial")
public class EdgeWithTwoElementsMap implements
		CrossFunction<Tuple2<Long, Long>, EdgeExtended<Long, Long, String, HashMap<String, String>>, EdgeExtended<Long, Long, String, HashMap<String, String>>> {

	private long newId = 1;
	private String label;
	
	public EdgeWithTwoElementsMap(String label) { this.label = label; }
	@Override
	public EdgeExtended<Long, Long, String, HashMap<String, String>> cross(
			Tuple2<Long, Long> vertexIdsOfEdge,
			EdgeExtended<Long, Long, String, HashMap<String, String>> maxId)
			throws Exception {
		
		EdgeExtended<Long, Long, String, HashMap<String, String>> edge = new EdgeExtended<>();
		edge.setEdgeId(maxId.f0 + newId);
		this.newId ++;
		
		edge.setSourceId(vertexIdsOfEdge.f0);
		edge.setTargetId(vertexIdsOfEdge.f1);
		
		edge.setLabel(this.label);
		
		HashMap<String, String> props = new HashMap<>();
		edge.setProps(props);
		return edge;
	}

}
