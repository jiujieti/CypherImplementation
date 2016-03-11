package MyProject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import MyProject.DataStructures.*;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BasicOperators {
	
	private final GraphExtended<Long, HashMap<String, String>, String,
	  HashMap<String, String>, String> graph;
	
	/*get the input graph*/
	public BasicOperators(GraphExtended<Long, HashMap<String, String>, String,
			  HashMap<String, String>, String> g) {
		this.graph = g;
		
	}	
	
	public void selectOnVertices(final String label){
		DataSet<VertexExtended<Long, HashMap<String, String>, String>> verticesSelectedByLabel = 
			graph.getAllVertices()
				 /*filter all vertices by label*/
				 .filter(
					new FilterFunction<VertexExtended<Long, HashMap<String, String>, String>>(){
					
						private static final long serialVersionUID = 1L;
						@Override
						public boolean filter(
							VertexExtended<Long, HashMap<String, String>, String> vertex)
							throws Exception {
						if(vertex.f2.equals(label) | label.isEmpty())
							return true;
						else return false;
					}
				});
	}
	
	public void selectOnVertices(final HashMap<String, String> props){
		DataSet<VertexExtended<Long, HashMap<String, String>, String>> verticesSelectedByProps = 
			graph.getAllVertices()
			/*filter all vertices by properties*/
				 .filter(
					new FilterFunction<VertexExtended<Long, HashMap<String, String>, String>>(){
						
						private static final long serialVersionUID = 1L;
						@Override
						public boolean filter(
							VertexExtended<Long, HashMap<String, String>, String> vertex)
									throws Exception {
							HashMap<String, String> propsOfVertex = vertex.f1;
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
	}
	public void selectOnVertices(String label, HashMap<String, String> props){
		
	}

	
	public void selectOnEdges(){
		
	}
	
	public static void projectOnVertices(){
	
	}
	
	public static void projectOnEdges(){
		
	}
	
	public void join(){
		
	}
	
	
}
