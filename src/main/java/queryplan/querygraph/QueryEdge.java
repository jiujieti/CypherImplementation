package queryplan.querygraph;

import java.util.HashMap;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Query Edge
 * A query edge consists of a label and a target vertex
 *  */
public class QueryEdge {
	private QueryVertex from, to;
	private String label;
	private HashMap<String, Tuple2<String, String>> props;
	private double priority;
	
	
	public QueryEdge(QueryVertex f, QueryVertex t, String l, HashMap<String, Tuple2<String, String>> ps) {
		from = f;
		to = t;
		label = l;
		props = ps;
		if(!l.equals("")) {
			priority += 0.5;
		}
		if(!props.isEmpty()) {
			priority += 1.5 * props.size();
		}
	}
	
	public QueryVertex getSourceVertex() {
		return from;
	}
	
	public QueryVertex getTargetVertex() {
		return to;
	}
	
	public String getLabel() {
		return label;
	}
	
	public HashMap<String, Tuple2<String, String>> getProps() {
		return props;
	}
	
	public double getPrio() {
		return priority;
	}
}
