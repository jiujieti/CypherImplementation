package operators.booleanExpressions.comparisons;

import java.util.HashMap;
import java.util.HashSet;

import operators.datastructures.VertexExtended;

import org.apache.flink.api.common.functions.FilterFunction;

@SuppressWarnings("serial")
public class LabelComparisonForVertices implements FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>{

	private String label;
	
	public LabelComparisonForVertices(String label) {
		this.label = label;
	}
	@Override
	public boolean filter(
			VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex)
			throws Exception {
		if(vertex.getLabels().contains(label) || label.equals("")) return true;
		else return false;
	}

}
