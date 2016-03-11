package MyProject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;


public class Filters {
	/*	Filter nodes by labels or properties separately
	 *  lb is used to select whether labels or properties are used */
	public static Graph<Long, HashMap<String, String>, HashMap<String, String>> filterOnNode
	(final Vertex<Long, HashMap<String, String>> n, 
			Graph<Long, HashMap<String, String>, HashMap<String, String>> g, Boolean lb){
		if(lb) 
			return g.filterOnVertices(
				new FilterFunction<Vertex<Long, HashMap<String, String>>>() {
					public boolean filter(Vertex<Long, HashMap<String, String>> vt) {
						Set<String> labelSet = VertexOperations.getLabels(n);
						Set<String> labelSetforVertex = new HashSet<> ();
						for(String s: vt.f1.get("labels").split(", "))
							labelSetforVertex.add(s);
						labelSetforVertex.retainAll(labelSet);
						if(!labelSetforVertex.isEmpty() | labelSet.isEmpty())	
							return true;
						else return false;
					}
				});
		else return g.filterOnVertices(
				new FilterFunction<Vertex<Long, HashMap<String, String>>>() {
					public boolean filter(Vertex<Long, HashMap<String, String>> vt) {
						HashMap<String, String> propMap = VertexOperations.getProps(n);
						HashMap<String, String> propforVertex = vt.f1;
						if(propMap.isEmpty())
							return true;
						for(Map.Entry<String, String> entry : propMap.entrySet()) {
							if(propforVertex.get(entry.getKey()) == null || !propforVertex.get(entry.getKey()).equals(entry.getValue()))
								return false;
						}
						return true;
					}
				});
	}
	
	/*	Filter nodes by labels and properties */
	public static Graph<Long, HashMap<String, String>, HashMap<String, String>> filterOnNode
	(final Vertex<Long, HashMap<String, String>> n, 
			Graph<Long, HashMap<String, String>, HashMap<String, String>> g){
		return g.filterOnVertices(
				new FilterFunction<Vertex<Long, HashMap<String, String>>>() {
					public boolean filter(Vertex<Long, HashMap<String, String>> vt) {
						Set<String> labelSet = VertexOperations.getLabels(n);
						Set<String> labelSetforVertex = new HashSet<> ();
						HashMap<String, String> propMap = VertexOperations.getProps(n);
						HashMap<String, String> propforVertex = vt.f1;
						
						/*Both labels and properties are absent*/
						if(propMap.isEmpty() && labelSet.isEmpty())
							return true;
						
						for(String s: vt.f1.get("labels").split(", "))
							labelSetforVertex.add(s);
						labelSetforVertex.retainAll(labelSet);
						
						if(labelSet.isEmpty() || 
								(!labelSet.isEmpty() && !labelSetforVertex.isEmpty())) {
							for(Map.Entry<String, String> entry : propMap.entrySet()) {
								if(propforVertex.get(entry.getKey()) == null || !propforVertex.get(entry.getKey()).equals(entry.getValue()))
									return false;
							}
							return true;
						} 
						return false;
					
					}
				});
	}
}
