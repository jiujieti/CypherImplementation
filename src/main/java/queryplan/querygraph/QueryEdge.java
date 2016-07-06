package queryplan.querygraph;
/**
 * Query Edge
 * A query edge consists of a label and a target vertex
 *  */
public class QueryEdge {
	QueryVertex from, to;
	String label;
	
	public QueryEdge(QueryVertex f, QueryVertex t, String l) {
		from = f;
		to = t;
		label = l;
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
}
