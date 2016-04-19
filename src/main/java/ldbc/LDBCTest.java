package ldbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import operators.*;
import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;
import operators.datastructures.VertexExtended;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;




@SuppressWarnings("serial")
public class LDBCTest {
	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		
		String dir = "C:/Users/s146508/Desktop/ubuntu/social_network/";

		DataSet<Tuple3<Long, String, String>> verticesFromFile = env.readCsvFile(dir + "vertices.csv")
				.fieldDelimiter("|")
				.types(Long.class, String.class, String.class);
		
		DataSet<Tuple5<Long, Long, Long, String, String>> edgesFromFile = env.readCsvFile(dir + "edges.csv")
				.fieldDelimiter("|")
				.types(Long.class, Long.class, Long.class, String.class, String.class);
		
		DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> vertices 
			= verticesFromFile.map(new VerticesFromFileToDataSet());
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> edges
			= edgesFromFile.map(new EdgesFromFileToDataSet());
		
		GraphExtended<Long, HashSet<String>, HashMap<String, String>, 
	      Long, String, HashMap<String, String>> graph = GraphExtended.fromDataSet(vertices, edges, env);
		
		
		// MATCH (m: person) - [:knows*1..2] -> (n: person)
		// RETURN n
		
		//Get all starting node ids whose labels are "person"
		ScanOperators scanOps = new ScanOperators(graph);
	    HashSet<String> q1 = new HashSet<>();
	    q1.add("person");
	    DataSet<ArrayList<Long>> startingVertexIds = scanOps.getInitialVerticesByLabels(q1);
	    
	    //[startingVertexId, Long.MAX_VALUE, endVertexId]
	    LabelMatchingOperators lengthRelationshipOps = new LabelMatchingOperators(graph, startingVertexIds);
	    String q2 = "knows";
	    DataSet<ArrayList<Long>> paths = lengthRelationshipOps.matchWithBounds(0, 1, 2, q2, JoinHint.REPARTITION_SORT_MERGE);
	   
	    
	    //filter endVertexIds above
	    UnaryOperators unaryOps = new UnaryOperators(graph, paths);
	    HashSet<String> q3 = new HashSet<>();
	    q3.add("person");
	    unaryOps.selectVerticesByLabels(2, q3);
	    
	   
	    //return results n (projection)
	    unaryOps.projectDistinctVertices(2).print();
	  
	}
	
	
	private static class VerticesFromFileToDataSet implements MapFunction<Tuple3<Long, String, String>, VertexExtended<Long, HashSet<String>, HashMap<String, String>>> {

		@Override
		public VertexExtended<Long, HashSet<String>, HashMap<String, String>> map(
				Tuple3<Long, String, String> vertexFromFile) throws Exception {
			
			VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex = new VertexExtended<Long, HashSet<String>, HashMap<String, String>>();
			
			vertex.setVertexId(vertexFromFile.f0);
			
			HashSet<String> labels = new HashSet<>();
			String[] labs = vertexFromFile.f1.substring(1, vertexFromFile.f1.length()- 1).split(",");
			for(String label: labs) {
				labels.add(label);
			}
			vertex.setLabels(labels);
			
			
			HashMap<String, String> properties = new HashMap<>();
			String pattern = "[^=]+=([^= ]*( |$))*";
			Pattern r = Pattern.compile(pattern);
			Matcher m = r.matcher(vertexFromFile.f2.substring(1, vertexFromFile.f2.length()-1));
			while(m.find()) {
				String[] keyAndValue = m.group(0).split("=");
				if(keyAndValue.length >= 2){
					String key = keyAndValue[0];
					String value = keyAndValue[1];
					if(value.length() >= 2){
						if(value.substring(value.length() - 2, value.length()).equals(", ")){
							properties.put(key, value.substring(0, value.length() - 2));
						}
						else{
							properties.put(key, value);
						}
					}
				}
				else {
					String key = keyAndValue[0];
					String value = "";
					properties.put(key, value);
				}
			}
			vertex.setProps(properties);
			
			return vertex;
			
		}
		
	}
	 //[^=]+=([^= ]*( |$))* 
	private static class EdgesFromFileToDataSet implements MapFunction<Tuple5<Long, Long, Long, String, String>, 
						EdgeExtended<Long, Long, String, HashMap<String, String>>> {

		@Override
		public EdgeExtended<Long, Long, String, HashMap<String, String>> map(
				Tuple5<Long, Long, Long, String, String> edgeFromFile) throws Exception {
			
			EdgeExtended<Long, Long, String, HashMap<String, String>> edge = new EdgeExtended<Long, Long, String, HashMap<String, String>>();
			
			edge.setEdgeId(edgeFromFile.f0);
			edge.setSourceId(edgeFromFile.f1);
			edge.setTargetId(edgeFromFile.f2);
			edge.setLabel(edgeFromFile.f3);

			HashMap<String, String> properties = new HashMap<>();
			if(edgeFromFile.f4.length() > 2){
				String[] keyAndValue = edgeFromFile.f4.substring(1, edgeFromFile.f4.length() - 2).split("=");
				if(keyAndValue.length >= 2) {
					properties.put(keyAndValue[0], keyAndValue[1]);
				}
			}
			edge.setProps(properties);
			
			return edge;
			
		}
		
	}
}
