package test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;
import operators.datastructures.VertexExtended;
import operators.*;

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.ExecutionEnvironment;

public class LabelMatchingOperatorsBetaTest {
	public static void main(String[] arg) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//		  env.setParallelism(8);
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
		HashSet<String> vl = new HashSet<>();
		String el = "Likes";
		 
		VertexExtended<Long, HashSet<String>, HashMap<String, String>> v1 = 
			new VertexExtended<> (1L, vl, vp1);
		VertexExtended<Long, HashSet<String>, HashMap<String, String>> v2 =
			new VertexExtended<> (2L, vl, vp2);
		VertexExtended<Long, HashSet<String>, HashMap<String, String>> v3 = 
			new VertexExtended<> (3L, vl, vp3);
		VertexExtended<Long, HashSet<String>, HashMap<String, String>> v4 = 
			new VertexExtended<> (4L, vl, vp4);

		EdgeExtended<Long, Long, String, HashMap<String, String>> e1 = 
			new EdgeExtended<> (101L, 1L, 2L, el, ep);
		EdgeExtended<Long, Long, String, HashMap<String, String>> e2 = 
			new EdgeExtended<> (102L, 2L, 3L, el, ep);
		EdgeExtended<Long, Long, String, HashMap<String, String>> e3 = 
			new EdgeExtended<> (103L, 3L, 4L, el, ep);
		//  EdgeExtended<String, Long, String, HashMap<String, String>> e4 =
			//	  new EdgeExtended<> ("e4", 4L, 1L, el, ep);
		EdgeExtended<Long, Long, String, HashMap<String, String>> e5 =
			new EdgeExtended<> (105L, 3L, 1L, el, ep);
		  
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> edgeList = 
			new ArrayList<>();
		edgeList.add(e1);
		edgeList.add(e2);
		edgeList.add(e3);
		//  edgeList.add(e4);
		edgeList.add(e5);
		  
		List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> vertexList = 
			new ArrayList<>();
		vertexList.add(v1);
		vertexList.add(v2);
		vertexList.add(v3);
		vertexList.add(v4);
		  
	    GraphExtended<Long, HashSet<String>, HashMap<String, String>, 
	     Long, String, HashMap<String, String>> graph = GraphExtended.fromCollection(vertexList, edgeList, env);
		 
	     //single input test
	    // ArrayList<Tuple2<String, Long>> init = new ArrayList<>();
	    // init.add(new Tuple2<String, Long>("e0", 4L));
	    // DataSet<ArrayList<Tuple2<String, Long>>> initial = env.fromElements(init);
	     //LabelMatchingOperators l = new LabelMatchingOperators(graph, initial);
	     //l.matchWithoutBounds(0, "Likes").print();
	     
	     // whole input test
	    ScanOperators s = new ScanOperators(graph);   
	    
	    
	   //s.getInitialVertices().print();
	    LabelMatchingOperatorsBeta l = new LabelMatchingOperatorsBeta(graph, s.getInitialVertices());
	    l.matchWithUpperBound(0, 1, "Likes", JoinHint.BROADCAST_HASH_FIRST).print();
	   // graph.getEdges().print();
	}

}
