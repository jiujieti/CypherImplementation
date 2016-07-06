package operators.booleanExpressions.comparisons;

import java.util.HashMap;

import operators.datastructures.EdgeExtended;

import org.apache.flink.api.common.functions.FilterFunction;

@SuppressWarnings("serial")
public class LabelComparisonForEdges implements FilterFunction<EdgeExtended<Long, Long, String, HashMap<String, String>>>{

	private String label;
	
	public LabelComparisonForEdges(String label) {this.label = label;}
	@Override
	public boolean filter(
			EdgeExtended<Long, Long, String, HashMap<String, String>> edge)
			throws Exception {
		if(edge.f3.equals(this.label) || label.equals("")) return true;
		else return false;
	}

}
