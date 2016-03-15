package MyProject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;

import MyProject.DataStructures.*;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

public class UnaryOperators {
	
	private GraphExtended<Long, ArrayList<String>, HashMap<String, String>, String,
	  String, HashMap<String, String>> graph;
	private int columnNumber;
	private DataSet<ArrayList<Tuple2<String, Long>>> vertexAndEdgeIds;
	
	/*get the input graph, current columnNumber and the vertex and edges Ids*/
	public UnaryOperators(GraphExtended<Long, ArrayList<String>, HashMap<String, String>, String,
			  String, HashMap<String, String>> g, int colNum,
			  DataSet<ArrayList<Tuple2<String, Long>>> verticesAndEdges) {
		this.graph = g;
		this.columnNumber = colNum;
		this.vertexAndEdgeIds = verticesAndEdges;
	}
	
	public UnaryOperators(DataSet<ArrayList<Tuple2<String, Long>>> verticesAndEdges) {
		this.vertexAndEdgeIds = verticesAndEdges;
	}
	
	/*No requirements on the current vertices*/
	public DataSet<ArrayList<Tuple2<String, Long>>> selectOnVertices() {
		return vertexAndEdgeIds;
	}
	
	public void selectOnVerticesByLabel(ArrayList<String> label){
	//	vertexAndEdgeIds.map();
		//this.vertexAndEdgeIds;
		/*DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> verticesSelectedByLabel = 
				graph.getAllVertices()
				 .filter(
					new FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>(){
					
						private static final long serialVersionUID = 1L;
						@Override
						public boolean filter(
							VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex)
							throws Exception {
						if(vertex.f1.contains(label)| label.isEmpty())
							return true;
						else return false;
					}
				});*/
		
	}
	
	/*public void selectOnVertices(final HashMap<String, String> props){
		DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> verticesSelectedByProps = 
			
			graph.getVertices()
			
				 .filter(
					new FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>(){
						
						private static final long serialVersionUID = 1L;
						@Override
						public boolean filter(
							VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex)
									throws Exception {
							HashMap<String, String> propsOfVertex = vertex.f2;
							if(props.isEmpty())
								return true;
							for(Map.Entry<String, String> entry : propsOfVertex.entrySet()) {
								if(propsOfVertex.get(entry.getKey()) == null || 
										!propsOfVertex.get(entry.getKey()).equals(entry.getValue()))
									return false;
							}
							return true;
						}
					});
	}*/
	public void selectOnVertices(String label, HashMap<String, String> props){
		
	}
	
	public void selectOnVertices(String[] label){
		
	}

	
	public void selectOnEdges(){
		
	}
	
	public static void projectOnVertices(){
	
	}
	
	public static void projectOnEdges(){
		
	}
		
}
