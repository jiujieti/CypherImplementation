package queryplan.querygraph;

//import java.util.ArrayList;

/**
 * Query Vertex
 * A query vertex consists of an array of labels and an array of adjacent edges
 *  */
public class QueryVertex {
	String label;
	QueryGraphComponent component;
	
	public QueryVertex(String s) {
		label = s;
	}
	
	public String getLabel() {
		return label;
	}
	
	public QueryGraphComponent getComponent() {
		return component;
	}
	
	public void setComponent(QueryGraphComponent qgc) {
		component = qgc;
	}
}