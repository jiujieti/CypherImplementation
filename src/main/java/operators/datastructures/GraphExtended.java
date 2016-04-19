package operators.datastructures;

import java.util.Collection;



import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Extended graph for Cypher Implementation
 * @param <K> the key type for vertex identifiers
 * @param <VL> the value type of vertex labels
 * @param <VP> the value type of vertex properties
 * @param <E> the key type for edge identifiers
 * @param <EL> the value type of edge label
 * @param <EP> the value type of edge properties
 * 
 */

public class GraphExtended<K, VL, VP, E, EL, EP> {
	
	/*adjacent lists might be added later*/
	private final DataSet<VertexExtended<K, VL, VP>> vertices;
	private final DataSet<EdgeExtended<E, K, EL, EP>> edges;
	private final ExecutionEnvironment context;
	
	/*initialization*/
	private GraphExtended(DataSet<VertexExtended<K, VL, VP>> vertices, 
				  DataSet<EdgeExtended<E, K, EL, EP>> edges, ExecutionEnvironment context) {
		this.vertices = vertices;
		this.edges = edges;
		this.context = context;
	}
	
	/*get all edges in a graph*/
	public DataSet<EdgeExtended<E, K, EL, EP>> getEdges(){
		return edges;
	}
	
	/*get all vertices in a graph*/
	public DataSet<VertexExtended<K, VL, VP>> getVertices(){
		return vertices;
	}
	
	/*get the environment*/
	public ExecutionEnvironment getExecutionEnvironment() {
		return context;
	}
	
	/*get all vertex IDs*/
	public DataSet<Tuple1<K>> getAllVertexIds() {
		DataSet<Tuple1<K>> vertexIds = vertices.map(new MapFunction<VertexExtended<K, VL, VP>, Tuple1<K>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple1<K> map(VertexExtended<K, VL, VP> vertex) throws Exception {
				return new Tuple1<K>(vertex.f0);
			}	
		});
		return vertexIds;
	}
	
	public DataSet<Tuple1<E>> getAllEdgeIds() {
		DataSet<Tuple1<E>> edgeIds = edges.map(new MapFunction<EdgeExtended<E, K, EL, EP>, Tuple1<E>>(){
			private static final long serialVersionUID = 1L;
			
			@Override
			public Tuple1<E> map(EdgeExtended<E, K, EL, EP> edge)
					throws Exception {
				return new Tuple1<E>(edge.f0);
			}
			
		});
		return edgeIds;
	}
	
	public static <K, VL, VP, E, EL, EP> GraphExtended<K, VL, VP, E, EL, EP> 
		fromCollection(Collection<VertexExtended<K, VL, VP>> vertices,
			Collection<EdgeExtended<E, K, EL, EP>> edges, ExecutionEnvironment context) {

		return fromDataSet(context.fromCollection(vertices),
				context.fromCollection(edges), context);
	}
	
	public static <K, VL, VP, E, EL, EP> GraphExtended<K, VL, VP, E, EL, EP> 
		fromDataSet(DataSet<VertexExtended<K, VL, VP>> vertices,
			DataSet<EdgeExtended<E, K, EL, EP>> edges, ExecutionEnvironment context) {

		return new GraphExtended<K, VL, VP, E, EL, EP>(vertices, edges, context);
	}
	/*NOT FINISHED YET*/
}
