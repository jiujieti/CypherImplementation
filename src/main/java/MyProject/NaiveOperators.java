package MyProject;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.Grouping;
import org.apache.flink.api.common.functions.FlatJoinFunction;




import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.HashMap;

public class NaiveOperators {
	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	/*	HashMap<String, String> myHashMap1 = new HashMap<>();
		HashMap<String, String> myHashMap2 = new HashMap<>();
		HashMap<String, String> myHashMap3 = new HashMap<>();
		HashMap<String, String> myHashMap4 = new HashMap<>();
		HashMap<String, String> myHashMap5 = new HashMap<>();
		HashMap<String, String> myHashMap6 = new HashMap<>();
		myHashMap1.put("labels", "follows");
		myHashMap2.put("labels", "likes");
		myHashMap3.put("labels", "follows");
		myHashMap4.put("labels", "");
		myHashMap5.put("labels", "");
		myHashMap6.put("labels", "");
		myHashMap4.put("name", "Bob");
		myHashMap5.put("age", "16");
		myHashMap5.put("address", "eindhoven");
		myHashMap6.put("age", "16");
		Vertex<Long, HashMap<String, String>> v1 = new Vertex<> (1L, myHashMap4);
		Vertex<Long, HashMap<String, String>> v2 = new Vertex<> (2L, myHashMap5);
		Vertex<Long, HashMap<String, String>> v3 = new Vertex<> (3L, myHashMap6);
		Edge<Long, HashMap<String, String>> e1 = new Edge<> (1L, 2L, myHashMap1);
		Edge<Long, HashMap<String, String>> e2 = new Edge<> (2L, 3L, myHashMap2);
		Edge<Long, HashMap<String, String>> e3 = new Edge<> (1L, 3L, myHashMap3);
		List<Edge<Long, HashMap<String, String>>> edgeList = new LinkedList<>();
		List<Vertex<Long, HashMap<String, String>>> vertexList = new LinkedList<>();
		edgeList.add(e1);
		edgeList.add(e2);
		edgeList.add(e3);
		vertexList.add(v1);
		vertexList.add(v2);
		vertexList.add(v3);
		Graph<Long, HashMap<String, String>, HashMap<String, String>> graph = Graph.fromCollection(vertexList, edgeList, env);*/
		
		HashMap<String, String> myHashMap1 = new HashMap<>();
		HashMap<String, String> myHashMap2 = new HashMap<>();
		HashMap<String, String> myHashMap3 = new HashMap<>();
		HashMap<String, String> myHashMap4 = new HashMap<>();
		HashMap<String, String> myHashMap5 = new HashMap<>();
		HashMap<String, String> myHashMap6 = new HashMap<>();
		HashMap<String, String> myHashMap7 = new HashMap<>();
	    
		HashMap<String, String> mHashMap1 = new HashMap<>();
		HashMap<String, String> mHashMap2 = new HashMap<>();
		HashMap<String, String> mHashMap3 = new HashMap<>();
		HashMap<String, String> mHashMap4 = new HashMap<>();
		HashMap<String, String> mHashMap5 = new HashMap<>();
	
		myHashMap1.put("labels", "");
		myHashMap2.put("labels", "");
		myHashMap3.put("labels", "");
		myHashMap4.put("labels", "");
		myHashMap5.put("labels", "");
		myHashMap6.put("labels", "");
		myHashMap7.put("labels", "");
		
		mHashMap1.put("labels", "likes");
		mHashMap2.put("labels", "dislikes");
		mHashMap3.put("labels", "dislikes");
		mHashMap4.put("labels", "likes");
		mHashMap5.put("labels", "dislikes");
		
		Vertex<Long, HashMap<String, String>> v1 = new Vertex<> (1L, myHashMap1);
		Vertex<Long, HashMap<String, String>> v2 = new Vertex<> (2L, myHashMap2);
		Vertex<Long, HashMap<String, String>> v3 = new Vertex<> (3L, myHashMap3);
		Vertex<Long, HashMap<String, String>> v4 = new Vertex<> (4L, myHashMap4);
		Vertex<Long, HashMap<String, String>> v5 = new Vertex<> (5L, myHashMap5);
		Vertex<Long, HashMap<String, String>> v6 = new Vertex<> (6L, myHashMap6);
		Vertex<Long, HashMap<String, String>> v7 = new Vertex<> (7L, myHashMap7);
		Edge<Long, HashMap<String, String>> e1 = new Edge<> (1L, 2L, mHashMap1);
		Edge<Long, HashMap<String, String>> e2 = new Edge<> (1L, 3L, mHashMap2);
		Edge<Long, HashMap<String, String>> e3 = new Edge<> (1L, 5L, mHashMap3);
		Edge<Long, HashMap<String, String>> e4 = new Edge<> (3L, 4L, mHashMap4);
		Edge<Long, HashMap<String, String>> e5 = new Edge<> (6L, 7L, mHashMap5);
		
		List<Edge<Long, HashMap<String, String>>> edgeList = new LinkedList<>();
		List<Vertex<Long, HashMap<String, String>>> vertexList = new LinkedList<>();
		edgeList.add(e1);
		edgeList.add(e2);
		edgeList.add(e3);
		edgeList.add(e4);
		edgeList.add(e5);
		vertexList.add(v1);
		vertexList.add(v2);
		vertexList.add(v3);
		vertexList.add(v4);
		vertexList.add(v5);
		vertexList.add(v6);
		vertexList.add(v7);
		
		Graph<Long, HashMap<String, String>, HashMap<String, String>> graph = Graph.fromCollection(vertexList, edgeList, env);
		Graph<Long, HashMap<String, String>, HashMap<String, String>> g1 = PathSelection.unitPath1(e1, graph);
		Graph<Long, HashMap<String, String>, HashMap<String, String>> g2 = PathSelection.unitPath1(e2, graph);
		//g2.getEdges().print();
		//Joins a = new Joins();
		
		//DataSet<Edge<Long, HashMap<String, String>>> K = a.joinE(g1, g2);
		//K.print();
		//Graph<Long, HashMap<String, String>, HashMap<String, String>> g3 = a.joinEdgesOnSource1(g1, g2, env);
		//g3.getEdges().print();
		//Graph<Long, HashMap<String, String>, HashMap<String, String>> g = PathSelection.unitPath(e2, graph);
		//g.getEdges().print();
		//g.intersect(graph, distinctEdges)	
		//Graph<Long, HashMap<String, String>, HashMap<String, String>> g2 = PathSelection.unitPath1(e2,graph);
	//	g2.getEdges().print();
		//g2.getVertices().print(); 
	/*	ListForDataSet<String> al1 = new ListForDataSet<>();
		al1.add("1L");
		al1.add("2L");
		ListForDataSet<String> al2 = new ListForDataSet<>();
		al1.add("2L");
		al1.add("3L");*/
		
		LinkedList<String> a1 = new LinkedList<>();
		a1.add("1L");
		a1.add("2L");
		LinkedList<String> a2 = new LinkedList<>();
		a2.add("1L");
		a2.add("3L");
		LinkedList<String> a3 = new LinkedList<>();
		a3.add("2L");
		a3.add("4L");
		
		Grouping<LinkedList<String>> s1 = env.fromElements(a1, a2).groupBy(
				new KeySelector<LinkedList<String>, String>()
				{
					public String getKey(LinkedList<String> ip){return ip.get(1);}
				});
		
		Grouping<LinkedList<String>> s2 = env.fromElements(a3).groupBy(
				new KeySelector<LinkedList<String>, String>()
				{
					public String getKey(LinkedList<String> ip){return ip.get(0);}
				});
		DataSet<LinkedList<String>> s3 = s1.getDataSet();
		DataSet<LinkedList<String>> s4 = s2.getDataSet();
		DataSet<LinkedList<String>> s = s3.join(s4).where(
				new  KeySelector<LinkedList<String>, String>()
				{
					public String getKey(LinkedList<String> ip){return ip.get(1);}
				}).equalTo(
				new  KeySelector<LinkedList<String>, String>()
				{
					public String getKey(LinkedList<String> ip){return ip.get(0);}
				}).with(
					new FlatJoinFunction<LinkedList<String>, LinkedList<String>, LinkedList<String>>()
					{
						@Override
						public void join(LinkedList<String> first,
								LinkedList<String> second,
								Collector<LinkedList<String>> out)
								throws Exception {
							first.addAll(second);
							out.collect(first);
						}
					});
	s.print();
	
	
	
    
	
//	DataSet<ListsForTriplets<Integer, Long, String>> d1 = env.fromElements(l1).groupBy()
	
	}
	
	
}

