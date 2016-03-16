package MyProject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;

import MyProject.DataStructures.EdgeExtended;
import MyProject.DataStructures.GraphExtended;
import MyProject.DataStructures.VertexExtended;

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
		  ArrayList<String> vl1 = new ArrayList<>();
		  vl1.add("Person");
		  vl1.add("User");
		  ArrayList<String> vl2 = new ArrayList<>();
		  vl2.add("Person");
		  String el1 = "Likes";
		  
		  VertexExtended<Long, ArrayList<String>, HashMap<String, String>> v1 = 
				  new VertexExtended<> (1L, vl1, vp1);
		  VertexExtended<Long, ArrayList<String>, HashMap<String, String>> v2 = 
				  new VertexExtended<> (2L, vl2, vp2);
		  EdgeExtended<String, Long, String, HashMap<String, String>> e1 = 
				  new EdgeExtended<> ("e1", 1L, 2L, el1, ep1);
		  
		  List<EdgeExtended<String, Long, String, HashMap<String, String>>> edgeList = 
				  new ArrayList<>();
		  edgeList.add(e1);
		  
		  List<VertexExtended<Long, ArrayList<String>, HashMap<String, String>>> vertexList = 
				  new ArrayList<>();
		  vertexList.add(v1);
		  vertexList.add(v2);
		  
	      GraphExtended<Long, ArrayList<String>, HashMap<String, String>, 
	      String, String, HashMap<String, String>> graph = GraphExtended.fromCollection(vertexList, edgeList, env);
		  
	      graph.getVertices().print();
	      graph.getEdges().print();
	      
	      ScanOperators s = new ScanOperators(graph);
	      UnaryOperators u = new UnaryOperators(graph, s.getInitialVertices());
	      
	      ArrayList<String> labels = new ArrayList<>();
	      labels.add("Person");
	      
	      //u.selectOnVerticesByLabels(labels, 0).print();
	      
	      HashMap<String, String> props = new HashMap<>();
	      props.put("name", "John");
	      props.put("age", "48");
	      //u.selectOnVerticesByProperties(props, 0).print();
	      u.selectVertices(labels, props, 0).print();
	      
	      
	}
}
