package queryplan.querygraph;

import java.util.HashMap;


import org.apache.flink.api.java.tuple.Tuple2;


//import java.util.ArrayList;

/**
 * Query Vertex
 * A query vertex consists of an array of labels and an array of adjacent edges
 *  */
public class QueryVertex {
	String label;
	HashMap<String, Tuple2<String, Double>> props;
	QueryGraphComponent component;
	double priority = 0;
	
	public QueryVertex(String s, HashMap<String, Tuple2<String, Double>> ps) {
		label = s;
		props = ps;
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
	
	public HashMap<String, Tuple2<String, Double>> getProps() {
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
}