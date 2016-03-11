package MyProject;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.flink.util.Collector; 
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.api.java.ExecutionEnvironment;

public class Joins {
	//Here coGroup must be used because a nested iteration is required potentially
	public Graph<Long, HashMap<String, String>, HashMap<String, String>> joinEdgesOnSource
	(Graph<Long, HashMap<String, String>, HashMap<String, String>> gorg,
			Graph<Long, HashMap<String, String>, HashMap<String, String>> gjoin,
			ExecutionEnvironment env) {
		DataSet<Edge<Long, HashMap<String, String>>> resEdges = gorg.getEdges()
				.coGroup(gjoin.getEdges())
				.where(0)
				.equalTo(0)
				.with(new CoGroupFunction<Edge<Long, HashMap<String, String>>, 
						Edge<Long, HashMap<String, String>>, Edge<Long, HashMap<String, String>>>(){
					public void coGroup(Iterable<Edge<Long, HashMap<String, String>>> eorg,
							Iterable<Edge<Long, HashMap<String, String>>> ejoin, 
							Collector<Edge<Long, HashMap<String, String>>> eres) {
						Iterator<Edge<Long, HashMap<String, String>>> eorgIt = eorg.iterator();
						Iterator<Edge<Long, HashMap<String, String>>> ejoinIt = ejoin.iterator();
						if(eorgIt.hasNext() && ejoinIt.hasNext() ){
							while(eorgIt.hasNext())
								eres.collect(eorgIt.next());
							while(ejoinIt.hasNext())
								eres.collect(ejoinIt.next());
							}
						}
					});
		return Graph.fromDataSet(gorg.getVertices(), resEdges, env);
	}
	
	public Graph<Long, HashMap<String, String>, HashMap<String, String>> joinEdgesOnSource1
	(Graph<Long, HashMap<String, String>, HashMap<String, String>> gorg,
			Graph<Long, HashMap<String, String>, HashMap<String, String>> gjoin,
			ExecutionEnvironment env) throws Exception {
		DataSet<Edge<Long, HashMap<String, String>>> resEdges = gorg.getEdges()
				.coGroup(gjoin.getEdges())
				.where(0)
				.equalTo(0)
				.with(new CoGroupFunction<Edge<Long, HashMap<String, String>>, 
						Edge<Long, HashMap<String, String>>, Edge<Long, HashMap<String, String>>>(){
					public void coGroup(Iterable<Edge<Long, HashMap<String, String>>> eorg,
							Iterable<Edge<Long, HashMap<String, String>>> ejoin, 
							Collector<Edge<Long, HashMap<String, String>>> eres) {
						Iterator<Edge<Long, HashMap<String, String>>> eorgIt = eorg.iterator();
						Iterator<Edge<Long, HashMap<String, String>>> ejoinIt = ejoin.iterator();
						if(!eorgIt.hasNext()){
							while(ejoinIt.hasNext())
								eres.collect(ejoinIt.next());
							}
						if(!ejoinIt.hasNext()){
							while(eorgIt.hasNext())
								eres.collect(eorgIt.next());
						}
					}});
		Graph<Long, HashMap<String, String>, HashMap<String, String>> gres = gorg.union(gjoin).removeEdges(resEdges.collect());
		return gres;
		//return Graph.fromDataSet(gorg.getVertices(), resEdges, env);
	}
	
	
	public Graph<Long, HashMap<String, String>, HashMap<String, String>> joinEdgesOnTarget
	(Graph<Long, HashMap<String, String>, HashMap<String, String>> gorg,
			Graph<Long, HashMap<String, String>, HashMap<String, String>> gjoin,
			ExecutionEnvironment env) {
		DataSet<Edge<Long, HashMap<String, String>>> resEdges = gorg.getEdges()
				.coGroup(gjoin.getEdges())
				.where(1)
				.equalTo(1)
				.with(new CoGroupFunction<Edge<Long, HashMap<String, String>>, 
						Edge<Long, HashMap<String, String>>, Edge<Long, HashMap<String, String>>>(){
					public void coGroup(Iterable<Edge<Long, HashMap<String, String>>> eorg,
							Iterable<Edge<Long, HashMap<String, String>>> ejoin, 
							Collector<Edge<Long, HashMap<String, String>>> eres) {
						Iterator<Edge<Long, HashMap<String, String>>> eorgIt = eorg.iterator();
						Iterator<Edge<Long, HashMap<String, String>>> ejoinIt = ejoin.iterator();
						if(eorgIt.hasNext()){
							eres.collect(new Edge<Long, HashMap<String, String>>(eorgIt.next().f0,
									eorgIt.next().f1, eorgIt.next().f2));
							}
						if(ejoinIt.hasNext()){
							eres.collect(new Edge<Long, HashMap<String, String>>(ejoinIt.next().f0,
									ejoinIt.next().f1, ejoinIt.next().f2));
							}
						}
					});
		return Graph.fromDataSet(gorg.getVertices(), resEdges, env);
	}
	
	public Graph<Long, HashMap<String, String>, HashMap<String, String>> joinEdges
	(Graph<Long, HashMap<String, String>, HashMap<String, String>> gorg,
			Graph<Long, HashMap<String, String>, HashMap<String, String>> gjoin,
			ExecutionEnvironment env) {
		DataSet<Edge<Long, HashMap<String, String>>> resEdges = gorg.getEdges()
				.coGroup(gjoin.getEdges())
				.where(1)
				.equalTo(0)
				.with(new CoGroupFunction<Edge<Long, HashMap<String, String>>, 
						Edge<Long, HashMap<String, String>>, Edge<Long, HashMap<String, String>>>(){
					public void coGroup(Iterable<Edge<Long, HashMap<String, String>>> eorg,
							Iterable<Edge<Long, HashMap<String, String>>> ejoin, 
							Collector<Edge<Long, HashMap<String, String>>> eres) {
						Iterator<Edge<Long, HashMap<String, String>>> eorgIt = eorg.iterator();
						Iterator<Edge<Long, HashMap<String, String>>> ejoinIt = ejoin.iterator();
						if(eorgIt.hasNext()){
							eres.collect(new Edge<Long, HashMap<String, String>>(eorgIt.next().f0,
									eorgIt.next().f1, eorgIt.next().f2));
							}
						if(ejoinIt.hasNext()){
							eres.collect(new Edge<Long, HashMap<String, String>>(ejoinIt.next().f0,
									ejoinIt.next().f1, ejoinIt.next().f2));
							}
						}
					});
		return Graph.fromDataSet(gorg.getVertices(), resEdges, env);
	}
}
