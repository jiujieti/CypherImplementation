package operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;
import operators.datastructures.VertexExtended;

import org.apache.flink.api.java.ExecutionEnvironment;

public class UnaryOperatorsTest {
	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		 
		  //properties for vertices and edges
		  HashMap<String, String> vp1 = new HashMap<>();
		  vp1.put("name", "John");
		  vp1.put("age", "48");
		  HashMap<String, String> vp2 = new HashMap<>();
		  vp2.put("name", "Alice");
		  vp2.put("age", "4");
		  vp2.put("gender", "female");
		  HashMap<String, String> ep1 = new HashMap<>();
		  ep1.put("time", "2016");
		  
		  //labels for vertices and edges
		  HashSet<String> vl1 = new HashSet<>();
		  vl1.add("Person");
		  vl1.add("User");
		  HashSet<String> vl2 = new HashSet<>();
		  vl2.add("Person");
		  String el1 = "Likes";
		  
		  VertexExtended<Long, HashSet<String>, HashMap<String, String>> v1 = 
				  new VertexExtended<> (1L, vl1, vp1);
		  VertexExtended<Long, HashSet<String>, HashMap<String, String>> v2 = 
				  new VertexExtended<> (2L, vl2, vp2);
		  EdgeExtended<Long, Long, String, HashMap<String, String>> e1 = 
				  new EdgeExtended<> (100L, 1L, 2L, el1, ep1);
		  
		  List<EdgeExtended<Long, Long, String, HashMap<String, String>>> edgeList = 
				  new ArrayList<>();
		  edgeList.add(e1);
		  
		  List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> vertexList = 
				  new ArrayList<>();
		  vertexList.add(v1);
		  vertexList.add(v2);
		  
	      GraphExtended<Long, HashSet<String>, HashMap<String, String>, 
	      Long, String, HashMap<String, String>> graph = GraphExtended.fromCollection(vertexList, edgeList, env);
		  
	 //     graph.getVertices().print();
	 //     graph.getEdges().print();
	      
	      ScanOperators s = new ScanOperators(graph);
	    //  UnaryOperators u = new UnaryOperators(graph, s.getInitialVertices());
	      
	      
	      HashSet<String> labels = new HashSet<>();
	      labels.add("Person");
	      
	     // u.selectVerticesByLabels(0, labels).print();
	     // HashMap<String, String> props = new HashMap<>();
	      //props.put("name", "John");
	      //props.put("age", "48");
	      //u.selectVerticesByProperties(0, props).print();
	      //u.selectVertices(0, labels, props).print();
	      
	    // test on edges?????  
	      
	      
	}
}
