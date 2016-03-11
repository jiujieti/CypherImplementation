package MyProject;

import java.util.HashMap;


import org.apache.flink.graph.Edge;

public class EdgeOperations{
	 
	  static String getLabel(Edge<Long, HashMap<String, String>> e) {
		  return e.f2.get("labels");
		 
	  }
	  
	  static HashMap<String, String> getProperties(Edge<Long, HashMap<String, String>> e) {
		  HashMap<String, String> props = new HashMap<>();
		  props = (HashMap<String, String>)e.f2.clone();
		  if(props.containsKey("labels")) 
			  props.remove("labels");
		  return props;
	  }
	  
	  
	/*  static boolean propExists(Edge<Long, HashMap<String, String>> e, String k, String v){
		  HashMap<String, String> props = new HashMap<>();
		  props = getProperties(e);
		  if()
	  }*/
	  /*public static Set<String> getLabels(Edge<Long, HashMap<String, String>> e) {
	  Set<String> edgeSet = new HashSet<>();
	  String labels = e.f2.get("labels");
	  if(labels.equals(""));
	  else {
		  for(String s: labels.split(", "))
		  edgeSet.add(s);	  		  
	  }
		  return edgeSet;
  }*/
}
