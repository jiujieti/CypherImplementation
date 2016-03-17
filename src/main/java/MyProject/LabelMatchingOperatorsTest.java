package MyProject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import MyProject.DataStructures.EdgeExtended;
import MyProject.DataStructures.GraphExtended;
import MyProject.DataStructures.VertexExtended;

public class LabelMatchingOperatorsTest {
	public static void main(String[] arg) throws Exception {
	  final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	 
	  //properties for vertices and edges
	  HashMap<String, String> vp1 = new HashMap<>();
	  vp1.put("name", "John");
	  HashMap<String, String> vp2 = new HashMap<>();
	  vp2.put("name", "Alice");
	  HashMap<String, String> vp3 = new HashMap<>();
	  vp3.put("name", "Bob");
	  HashMap<String, String> vp4 = new HashMap<>();
	  vp4.put("name", "Amy");
	  HashMap<String, String> ep = new HashMap<>();
	  
	  //labels for vertices and edges
	  ArrayList<String> vl = new ArrayList<>();
	  String el = "Likes";
	 
	  VertexExtended<Long, ArrayList<String>, HashMap<String, String>> v1 = 
			  new VertexExtended<> (1L, vl, vp1);
	  VertexExtended<Long, ArrayList<String>, HashMap<String, String>> v2 = 
			  new VertexExtended<> (2L, vl, vp2);
	  VertexExtended<Long, ArrayList<String>, HashMap<String, String>> v3 = 
			  new VertexExtended<> (3L, vl, vp3);
	  VertexExtended<Long, ArrayList<String>, HashMap<String, String>> v4 = 
			  new VertexExtended<> (4L, vl, vp4);
			  		  
	  EdgeExtended<String, Long, String, HashMap<String, String>> e1 = 
			  new EdgeExtended<> ("e1", 1L, 2L, el, ep);
	  EdgeExtended<String, Long, String, HashMap<String, String>> e2 = 
			  new EdgeExtended<> ("e2", 2L, 3L, el, ep);
	  EdgeExtended<String, Long, String, HashMap<String, String>> e3 = 
			  new EdgeExtended<> ("e3", 3L, 4L, el, ep);
	  
	  List<EdgeExtended<String, Long, String, HashMap<String, String>>> edgeList = 
			  new ArrayList<>();
	  edgeList.add(e1);
	  edgeList.add(e2);
	  edgeList.add(e3);
	  
	  
	  List<VertexExtended<Long, ArrayList<String>, HashMap<String, String>>> vertexList = 
			  new ArrayList<>();
	  vertexList.add(v1);
	  vertexList.add(v2);
	  vertexList.add(v3);
	  vertexList.add(v4);
	  
     GraphExtended<Long, ArrayList<String>, HashMap<String, String>, 
     String, String, HashMap<String, String>> graph = GraphExtended.fromCollection(vertexList, edgeList, env);
	 
     //single input test
     ArrayList<Tuple2<String, Long>> init = new ArrayList<>();
     init.add(new Tuple2<String, Long>("e0", 1L));
     DataSet<ArrayList<Tuple2<String, Long>>> initial = env.fromElements(init);
  // LabelMatchingOperators l = new LabelMatchingOperators(graph, initial);
     
     
     // whole input test
     ScanOperators s = new ScanOperators(graph);    
    s.getInitialVertices().print();
    LabelMatchingOperators l = new LabelMatchingOperators(graph, s.getInitialVertices());
    l.matchWithoutBounds(0, "Likes").print();
    }
}
