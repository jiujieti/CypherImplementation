package MyProject.DataStructures;

import java.util.HashMap;
import java.util.ArrayList;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
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
	
	
	/*adjacent lists migth be added later*/
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
	public DataSet<EdgeExtended<E, K, EL, EP>> getAllEdges(){
		return this.edges;
	}
	
	/*get all vertices in a graph*/
	public DataSet<VertexExtended<K, VL, VP>> getAllVertices(){
		return this.vertices;
	}
	
	/*Obtain initial pairs of connected vertex IDs in the graph through edges*/
	/*public DataSet<LinkedList<K>> getInitialEdges(){
		DataSet<LinkedList<K>> initialEdges = edges.map(
				new MapFunction<EdgeExtended<K, EV, EE>, LinkedList<K>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public LinkedList<K> map(EdgeExtended<K, EV, EE> edge){
						LinkedList<K> linkedList = new LinkedList<K>();
						linkedList.add(edge.f0);
						linkedList.add(edge.f1);
						return linkedList;
					}});
		return initialEdges;
	}*/
	/*Obtain initial vertex IDs in the graph*/
	public DataSet<ArrayList<K>> getInitialVertexIds(){
		DataSet<ArrayList<K>> initialVertexIds = vertices.map(
				new MapFunction<VertexExtended<K, VL, VP>, ArrayList<K>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public ArrayList<K> map(VertexExtended<K, VL, VP> vertex){
						ArrayList<K> vertexId = new ArrayList<K>();
						vertexId.add(vertex.f0);
						return vertexId;
					}});
		return initialVertexIds;
	}
	
	/*NOT FINISHED YET*/
}
