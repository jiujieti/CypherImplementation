package MyProject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.graph.Vertex;

public class VertexOperations {
	  public static Set<String> getLabels(Vertex<Long, HashMap<String, String>> v) {
		  Set<String> vertexSet = new HashSet<>();
		  if(v.f1.get("labels").equals(""));
		  else {
			  for(String s: v.f1.get("labels").split(", "))
				  vertexSet.add(s);
		  }
		  return vertexSet;
	  }
	  
	  public static String getLabel(Vertex<Long, HashMap<String, String>> v) {
		  return v.f1.get("labels");
	  }
	  
	  public static HashMap<String, String> getProps(Vertex<Long, HashMap<String, String>> v) {
		  HashMap<String, String> props = new HashMap<>();
		  props = (HashMap<String, String>)v.f1.clone();
		  if(props.containsKey("labels")) 
			  props.remove("labels");
		  return props;
	  }
	}
