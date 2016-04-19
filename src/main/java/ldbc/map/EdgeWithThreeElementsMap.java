package ldbc.map;

import java.util.HashMap;

import operators.datastructures.EdgeExtended;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class EdgeWithThreeElementsMap implements
	CrossFunction<Tuple3<Long, Long, String>, EdgeExtended<Long, Long, String, HashMap<String, String>>, EdgeExtended<Long, Long, String, HashMap<String, String>>> {

	private long newId = 1;
	private String label;
	private String key;

	public EdgeWithThreeElementsMap(String label, String key) { this.label = label; this.key = key;}

	@Override
	public EdgeExtended<Long, Long, String, HashMap<String, String>> cross(
			Tuple3<Long, Long, String> vertexIdsOfEdge,
			EdgeExtended<Long, Long, String, HashMap<String, String>> maxId)
					throws Exception {

		EdgeExtended<Long, Long, String, HashMap<String, String>> edge = new EdgeExtended<>();
		edge.setEdgeId(maxId.f0 + newId);
		this.newId ++;

		edge.setSourceId(vertexIdsOfEdge.f0);
		edge.setTargetId(vertexIdsOfEdge.f1);

		edge.setLabel(this.label);
		
		HashMap<String, String> prop = new HashMap<>();
		prop.put(this.key, vertexIdsOfEdge.f2);
		edge.setProps(prop);
		return edge;
}

}
