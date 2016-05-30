package ldbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import operators.*;
import operators.booleanExpressions.OR;
import operators.booleanExpressions.comparisons.LabelComparisonForVertices;
import operators.booleanExpressions.comparisons.PropertyFilterForVertices;
import operators.booleanExpressions.comparisons.PropertyMatchForVertices;
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
		
		env.setParallelism(8);
		String dir = "C:/Users/s146508/Desktop/ubuntu/35kPerson/";

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
		/*ScanOperators scanOps = new ScanOperators(graph);
	    HashSet<String> q1 = new HashSet<>();
	    q1.add("person");
	    DataSet<ArrayList<Long>> startingVertexIds = scanOps.getInitialVerticesByLabels(q1);
	    
	    //[startingVertexId, endVertexId]
	    LabelMatchingOperators lengthRelationshipOps = new LabelMatchingOperators(graph, startingVertexIds);
	    String q2 = "knows";
	    DataSet<ArrayList<Long>> paths = lengthRelationshipOps.matchWithBounds(0, 1, 2, q2, JoinHint.REPARTITION_SORT_MERGE);
	   
	    
	    //filter endVertexIds above
	    UnaryOperators unaryOps = new UnaryOperators(graph, paths);
	    HashSet<String> q3 = new HashSet<>();
	    q3.add("person");
	    unaryOps.selectVerticesByLabels(1, q3);
	    
	   
	    //return results n (projection)
	   unaryOps.projectDistinctVertices(1).print();*/
		
		
		//MATCH (m:comment) - [] -> (n:person)
		//RETURN n
		/*ScanOperators scanOps = new ScanOperators(graph);
	    HashSet<String> q1 = new HashSet<>();
	    q1.add("comment");
	    DataSet<ArrayList<Long>> paths = scanOps.getInitialVerticesByLabels(q1);
	    
	    UnaryOperators unaryOps = new UnaryOperators(graph, paths);
	    unaryOps.selectEdgesOnRightSide(0, JoinHint.BROADCAST_HASH_FIRST);
	    
	   
	    HashMap<String, String> q3 = new HashMap<>();
	    q3.put("browserUsed", "Chrome");
	    //HashSet<String> q3 = new HashSet<>();
	    //q3.add("person");
	    unaryOps.selectVerticesByProperties(2, q3);
	    
	    unaryOps.projectDistinctVertices(2).print();*/
		
		//simple select m: comment
		/*ScanOperators scanOps = new ScanOperators(graph);
	    HashSet<String> q1 = new HashSet<>();
	    q1.add("comment");
	    DataSet<ArrayList<Long>> paths = scanOps.getInitialVerticesByLabels(q1);
	    paths.print();*/
		
		//m: comment - [:hasCreator]
		/*ScanOperators scanOps = new ScanOperators(graph);
	    HashSet<String> q1 = new HashSet<>();
	    q1.add("comment");
	    DataSet<ArrayList<Long>> paths = scanOps.getInitialVerticesByLabels(q1);
	    
	    UnaryOperators unaryOps = new UnaryOperators(graph, paths);
	    unaryOps.selectOutEdgesByLabel(0, "hasCreator", JoinHint.BROADCAST_HASH_FIRST).print();*/
	    
		
		
		// query 9
		/*ScanOperators scanOps = new ScanOperators(graph);
	    HashSet<String> q1 = new HashSet<>();
	    q1.add("person");
	    DataSet<ArrayList<Long>> paths = scanOps.getInitialVerticesByLabels(q1);
	    
	    UnaryOperators unaryOps = new UnaryOperators(graph, paths);
	    
	
	    unaryOps.selectOutEdgesByLabel(0, "likes", JoinHint.BROADCAST_HASH_FIRST);
	    
	    HashMap<String, String> props = new HashMap<>();
	    props.put("browserUsed", "Chrome");
	    AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> q2 = new AND<>
	    (new PropertyMatchForVertices(props), new PropertyFilterForVertices("length", ">", 50));
	    AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> q3 = new AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>
	    (new LabelComparisonForVertices("comment"), q2);
	    
	    unaryOps.selectVerticesByBooleanExpressions(2, q3);
	    
	    unaryOps.projectDistinctVertices(2).print();*/
		
	    //query 10
	    /*ScanOperators scanOps = new ScanOperators(graph);
	    HashMap<String, String> props = new HashMap<>();
	    props.put("browserUsed", "Chrome");
	    AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> q1 = new AND<>
	    (new PropertyMatchForVertices(props), new PropertyFilterForVertices("length", ">", 50));
	    AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> q2 = new AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>
	    (new LabelComparisonForVertices("comment"), q1);
	    
	    DataSet<ArrayList<Long>> paths = scanOps.getInitialVerticesByBooleanExpressions(q2);
	    
	    UnaryOperators unaryOps = new UnaryOperators(graph, paths);
	    unaryOps.selectInEdgesByLabel(0, "likes", JoinHint.BROADCAST_HASH_FIRST);
	    
	    HashSet<String> q3 = new HashSet<>();
	    q3.add("person");
	    unaryOps.selectVerticesByLabels(2, q3);
	
	    unaryOps.projectDistinctVertices(0).print();*/
		
		//query11
		ScanOperators scanOps1 = new ScanOperators(graph);
	    HashSet<String> q1 = new HashSet<>();
	    q1.add("comment");
	    DataSet<ArrayList<Long>> paths1 = scanOps1.getInitialVerticesByLabels(q1);
	    
	    UnaryOperators unaryOps1 = new UnaryOperators(graph, paths1);
	
	    unaryOps1.selectOutEdgesByLabel(0, "hasCreator", JoinHint.BROADCAST_HASH_FIRST);
	    
	    HashSet<String> q2 = new HashSet<>();
	    q2.add("person");
	    DataSet<ArrayList<Long>> result1 = unaryOps1.selectVerticesByLabels(2, q2);
	    
	    ScanOperators scanOps2 = new ScanOperators(graph);
	    HashSet<String> q3 = new HashSet<>();
	    q3.add("post");
	    DataSet<ArrayList<Long>> paths2 = scanOps2.getInitialVerticesByLabels(q3);
	    
	    UnaryOperators unaryOps2 = new UnaryOperators(graph, paths2);
		
	    DataSet<ArrayList<Long>> result2 = unaryOps2.selectOutEdgesByLabel(0, "hasCreator", JoinHint.BROADCAST_HASH_FIRST);
	    
	    BinaryOperators binaryOps = new BinaryOperators(result1, result2);
	    DataSet<ArrayList<Long>> result = binaryOps.JoinOnAfterVertices(2, 2);
	    
	    UnaryOperators unaryOps = new UnaryOperators(graph, result);
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
