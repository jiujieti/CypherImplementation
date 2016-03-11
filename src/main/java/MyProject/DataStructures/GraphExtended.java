package MyProject.DataStructures;

import java.util.HashMap;
import java.util.ArrayList;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;



/**
 * Extended graph for Cypher Implementation
 * @param <K> the key type for edge and vertex identifiers
 * @param <VV> the value type of vertex properties
 * @param <VE> the value type of vertex label
 * @param <EV> the value type of edge properties
 * @param <EE> the value type of edge label
 */

public class GraphExtended<K, VV extends HashMap<String, String>, VE, 
							  EV extends HashMap<String, String>, EE> {
	
	
	/*adjacent lists migth be added later*/
	private final DataSet<VertexExtended<K, VV, VE>> vertices;
	private final DataSet<EdgeExtended<K, EV, EE>> edges;
	private final ExecutionEnvironment context;
	
	/*initialization*/
	private GraphExtended(DataSet<VertexExtended<K, VV, VE>> vertices, 
				  DataSet<EdgeExtended<K, EV, EE>> edges, ExecutionEnvironment context) {
		this.vertices = vertices;
		this.edges = edges;
		this.context = context;
	}
	
	/*get all edges in a graph*/
	public DataSet<EdgeExtended<K, EV, EE>> getAllEdges(){
		return this.edges;
	}
	
	/*get all vertices in a graph*/
	public DataSet<VertexExtended<K, VV, VE>> getAllVertices(){
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
				new MapFunction<VertexExtended<K, VV, VE>, ArrayList<K>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public ArrayList<K> map(VertexExtended<K, VV, VE> vertex){
						ArrayList<K> vertexId = new ArrayList<K>();
						vertexId.add(vertex.f0);
						return vertexId;
					}});
		return initialVertexIds;
	}
	
	/*NOT FINISHED YET*/
}
