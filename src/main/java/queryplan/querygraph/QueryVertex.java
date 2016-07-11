package queryplan.querygraph;

import java.util.HashMap;


import org.apache.flink.api.java.tuple.Tuple2;


//import java.util.ArrayList;

/**
 * Query Vertex
 * A query vertex consists of an array of labels and an array of adjacent edges
 *  */
public class QueryVertex {
	private String label;
	private	HashMap<String, Tuple2<String, String>> props;
	private QueryGraphComponent component;
	private double priority = 0;
	private boolean isReturnedValue = false;
	
	public QueryVertex(String s, HashMap<String, Tuple2<String, String>> ps, boolean rv) {
		label = s;
		props = ps;
		isReturnedValue = rv;
		if(!s.equals("")) {
			priority += 0.7;
		}
		if(!props.isEmpty()) {
			priority += props.size();
		}
	}
	
	public String getLabel() {
		return label;
	}
	
	public HashMap<String, Tuple2<String, String>> getProps() {
		return props;
	}
	
	public QueryGraphComponent getComponent() {
		return component;
	}
	
	public void setComponent(QueryGraphComponent qgc) {
		component = qgc;
	}

	public double getPrio() {
		return priority;
	}
	
	public boolean isOutput() {
		return isReturnedValue;
	}
}