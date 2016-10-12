package queryplan.querygraph;
/**
 * Query Graph
 * A query graph consists of an array of vertices and an array of edges
 *
 */

public class QueryGraph {
	QueryVertex[] vertices;
	QueryEdge[] edges;
	
	public QueryGraph(QueryVertex[] vs, QueryEdge[] es) {
		vertices = vs;
		edges = es;
	}
	
	public QueryEdge[] getQueryEdges() {
		return this.edges;
	}
	
	public QueryVertex[] getQueryVertices() {
		return this.vertices;
	}
}
