package MyProject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.List;


import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.Graph;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;



public class PathSelection {
	/*public static Graph<Long, HashMap<String, String>, HashMap<String, String>> unitPath
	(final Edge<Long, HashMap<String, String>> pt,
		Graph<Long, HashMap<String, String>, HashMap<String, String>> g){
		return g.filterOnEdges(
				new FilterFunction<Edge<Long, HashMap<String, String>>>() {
					public boolean filter(Edge<Long, HashMap<String, String>> ed) {
						String label = EdgeOperations.getLabel(pt);
						Set<String> labelSetforEdge = new HashSet<> ();
						for(String s: ed.f2.get("labels").split(", "))
							labelSetforEdge.add(s);
						if(labelSetforEdge.contains(label))
							return true;
						else return false;
					}
				});	
	}*/
	
	
	
	public static Graph<Long, HashMap<String, String>, HashMap<String, String>> unitPath
	(final Edge<Long, HashMap<String, String>> pt,
		Graph<Long, HashMap<String, String>, HashMap<String, String>> g){
		return g.filterOnEdges(
				new FilterFunction<Edge<Long, HashMap<String, String>>>() {
					public boolean filter(Edge<Long, HashMap<String, String>> ed) {
						String label = EdgeOperations.getLabel(pt);
						Set<String> labelSetforEdge = new HashSet<> ();
						for(String s: ed.f2.get("labels").split(", "))
							labelSetforEdge.add(s);
						if(labelSetforEdge.contains(label))
							return true;
						else return false;
					}
				});	
	}
	
	public static Graph<Long, HashMap<String, String>, HashMap<String, String>> unitPath1
	(final Edge<Long, HashMap<String, String>> pt,
		Graph<Long, HashMap<String, String>, HashMap<String, String>> g) throws Exception{
		Graph<Long, HashMap<String, String>, HashMap<String, String>> interg =  g.filterOnEdges(
				new FilterFunction<Edge<Long, HashMap<String, String>>>() {
					public boolean filter(Edge<Long, HashMap<String, String>> ed) {
						String label = EdgeOperations.getLabel(pt);
						Set<String> labelSetforEdge = new HashSet<> ();
						for(String s: ed.f2.get("labels").split(", "))
							labelSetforEdge.add(s);
						if(labelSetforEdge.contains(label))
							return true;
						else return false;
					}
				});
		
		DataSet<Tuple3<Long, Long, Long>> vs = interg.inDegrees()
				.join(interg.outDegrees())
				.where(0)
				.equalTo(0)
				.with(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple3<Long, Long, Long>>(){
							public Tuple3<Long, Long, Long> join(Tuple2<Long, Long> v1, Tuple2<Long, Long> v2){
								return new Tuple3<Long, Long, Long>(v1.f0, v1.f1, v2.f1);
								}
							});
		
		DataSet<Tuple1<Long>> vd = vs.flatMap(new FlatMapFunction<Tuple3<Long, Long, Long>, Tuple1<Long>>(){
			public void flatMap(Tuple3<Long, Long, Long> vin, Collector<Tuple1<Long>> vout){
				if(vin.f1 == 0 && vin.f2 == 0)
					vout.collect(new Tuple1<Long>(vin.f0));
			}
		});
		
		DataSet<Vertex<Long, HashMap<String, String>>> verticesRemoved = vd
				.join(interg.getVertices())
				.where(0)
				.equalTo(0)
				.with(new JoinFunction<Tuple1<Long>, Vertex<Long, HashMap<String, String>>, 
						Vertex<Long, HashMap<String, String>>>() {
					public Vertex<Long, HashMap<String, String>> join(Tuple1<Long> vid, 
																Vertex<Long, HashMap<String, String>> vset){
						return new Vertex<Long, HashMap<String, String>>(vid.f0, vset.f1);
					}
				});
		
		List<Vertex<Long, HashMap<String, String>>> vertList = verticesRemoved.collect();
		if(!vertList.isEmpty())
			return interg.removeVertices(vertList);
		return interg;
		
	}
}
