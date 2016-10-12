package queryplan.querygraph;

import java.util.HashMap;


import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Query Vertex
 * A query vertex consists of an array of labels and an array of adjacent edges
 *  */
public class QueryVertex {

	//indicate the label specified for the vertex in a query
	private String label;

	//indicate the properties specified for the vertex in a query
	private	HashMap<String, Tuple2<String, String>> props;

	//indicate which graph component this vertex belongs to
	private QueryGraphComponent component;

    //indicate the selectivity of this query vertex
	private double priority = 0;

	//indicate whether the selected vertices determined by this query vertex should be returned or not
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