package ldbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import operators.*;
import operators.booleanExpressions.AND;
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
import org.apache.flink.core.fs.FileSystem.WriteMode;

/*Test queries for LDBC generated database */
@SuppressWarnings("serial")
public class LDBCTest {
	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	
		String dir = args[0];
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
		
		switch(args[1]) {
			case "1": {
				//MATCH (m:person) no output linear
				ScanOperators scanOps = new ScanOperators(graph);
			    HashSet<String> q1 = new HashSet<>();
			    q1.add("person");
			    DataSet<ArrayList<Long>> startingVertexIds = scanOps.getInitialVerticesByLabels(q1);
			    startingVertexIds.writeAsText(args[2], WriteMode.OVERWRITE);
			    env.execute();
			    break;
			}
			case "2": {
				//MATCH () - [:hasCreator] -> () no output linear
				ScanOperators scanOps = new ScanOperators(graph);
			    DataSet<ArrayList<Long>> paths = scanOps.getInitialVertices();
			    UnaryOperators unaryOps = new UnaryOperators(graph, paths);
			    DataSet<ArrayList<Long>> results = unaryOps.selectOutEdgesByLabel(0, "hasCreator", JoinHint.BROADCAST_HASH_FIRST);
			    results.writeAsText(args[2], WriteMode.OVERWRITE);
			    env.execute();
			   
			    break;
			}
			case "3": {
				//MATCH (m:person) - [] -> (n:comment)
				//RETURN m
				ScanOperators scanOps = new ScanOperators(graph);
				HashSet<String> q1 = new HashSet<>();
			    q1.add("person");
			    DataSet<ArrayList<Long>> paths = scanOps.getInitialVerticesByLabels(q1);

				UnaryOperators unaryOps = new UnaryOperators(graph, paths);
			    unaryOps.selectOutEdges(0, JoinHint.BROADCAST_HASH_FIRST);
			    
			    
			    HashSet<String> q2 = new HashSet<>();
			    q2.add("comment");
			    unaryOps.selectVerticesByLabels(2, q2);
			    
			    unaryOps.projectDistinctVertices(0).writeAsText(args[2], WriteMode.OVERWRITE);
			    env.execute();
			    break;
			}
			case "4": {
				//MATCH (m:person) - [] -> (n:comment)
				//RETURN m			
				ScanOperators scanOps = new ScanOperators(graph);
				HashSet<String> q1 = new HashSet<>();
			    q1.add("comment");
			    DataSet<ArrayList<Long>> paths = scanOps.getInitialVerticesByLabels(q1);

				UnaryOperators unaryOps = new UnaryOperators(graph, paths);
			    unaryOps.selectInEdges(0, JoinHint.BROADCAST_HASH_FIRST);
			    
			    
			    HashSet<String> q2 = new HashSet<>();
			    q2.add("person");
			    unaryOps.selectVerticesByLabels(2, q2);
			    
			    unaryOps.projectDistinctVertices(2).writeAsText(args[2], WriteMode.OVERWRITE);
			    env.execute();
			    break;

			}
			case "5": {
				//MATCH (m:person) - [:likes] - (n:comment)
				//WHERE n.length > `50'
				//RETURN m
				ScanOperators scanOps = new ScanOperators(graph);
			    HashSet<String> q1 = new HashSet<>();
			    q1.add("person");
			    DataSet<ArrayList<Long>> paths = scanOps.getInitialVerticesByLabels(q1);
			    UnaryOperators unaryOps = new UnaryOperators(graph, paths);
			    unaryOps.selectOutEdgesByLabel(0, "likes", JoinHint.BROADCAST_HASH_FIRST);
	
			    AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> q2 = new AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>
			    (new LabelComparisonForVertices("comment"), new PropertyFilterForVertices("length", ">", "50"));
			    
			    unaryOps.selectVerticesByBooleanExpressions(2, q2, JoinHint.BROADCAST_HASH_FIRST);
			    unaryOps.projectDistinctVertices(2).writeAsText(args[2], WriteMode.OVERWRITE);
			    env.execute();
			    break;
			}
			case "6": {
				//MATCH (m:person) - [:likes] - (n:comment)
				//WHERE n.length > `50' AND n.browserUsed = `Chrome'
				//RETURN n
				ScanOperators scanOps = new ScanOperators(graph);
			    HashSet<String> q1 = new HashSet<>();
			    q1.add("person");
			    DataSet<ArrayList<Long>> paths = scanOps.getInitialVerticesByLabels(q1);
			    
			    UnaryOperators unaryOps = new UnaryOperators(graph, paths);
			    
			
			    unaryOps.selectOutEdgesByLabel(0, "likes", JoinHint.BROADCAST_HASH_FIRST);
			    
			    HashMap<String, String> props = new HashMap<>();
			    props.put("browserUsed", "Chrome");
			    AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> q2 = new AND<>
			    (new PropertyMatchForVertices(props), new PropertyFilterForVertices("length", ">", "50"));
			    AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> q3 = new AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>
			    (new LabelComparisonForVertices("comment"), q2);
			    
			    unaryOps.selectVerticesByBooleanExpressions(2, q3, JoinHint.BROADCAST_HASH_FIRST);
			    
			    unaryOps.projectDistinctVertices(2).writeAsText(args[2], WriteMode.OVERWRITE);
			    env.execute();
			    break;
			}
			case "7": {
				//MATCH (m:person) - [:likes] - (n:comment)
				//WHERE n.length > `50' OR n.browserUsed = `Chrome'
				//RETURN n
				ScanOperators scanOps = new ScanOperators(graph);
			    HashSet<String> q1 = new HashSet<>();
			    q1.add("person");
			    DataSet<ArrayList<Long>> paths = scanOps.getInitialVerticesByLabels(q1);
			    
			    UnaryOperators unaryOps = new UnaryOperators(graph, paths);
			    
			
			    unaryOps.selectOutEdgesByLabel(0, "likes", JoinHint.BROADCAST_HASH_FIRST);
			    
			    HashMap<String, String> props = new HashMap<>();
			    props.put("browserUsed", "Chrome");
			    OR<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> q2 = new OR<>
			    (new PropertyMatchForVertices(props), new PropertyFilterForVertices("length", ">", "50"));
			    AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> q3 = new AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>
			    (new LabelComparisonForVertices("comment"), q2);
			    
			    unaryOps.selectVerticesByBooleanExpressions(2, q3, JoinHint.BROADCAST_HASH_FIRST);
			    
			    unaryOps.projectDistinctVertices(2).writeAsText(args[2], WriteMode.OVERWRITE);
			    env.execute();
			    break;
			}
			
			case "8" : {
				//MATCH (m:person) - [:likes] - (n:comment)
				//WHERE n.length > `50' AND n.browserUsed = `Chrome'
				//RETURN n
				//In reverse order
				ScanOperators scanOps = new ScanOperators(graph);
			    HashMap<String, String> props = new HashMap<>();
			    props.put("browserUsed", "Chrome");
			    AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> q1 = new AND<>
			    (new PropertyMatchForVertices(props), new PropertyFilterForVertices("length", ">", "50"));
			    AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> q2 = new AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>
			    (new LabelComparisonForVertices("comment"), q1);
			    
			    DataSet<ArrayList<Long>> paths = scanOps.getInitialVerticesByBooleanExpressions(q2);
			    
			    UnaryOperators unaryOps = new UnaryOperators(graph, paths);
			    unaryOps.selectInEdgesByLabel(0, "likes", JoinHint.BROADCAST_HASH_FIRST);
			    
			    HashSet<String> q3 = new HashSet<>();
			    q3.add("person");
			    unaryOps.selectVerticesByLabels(2, q3);
			
			    unaryOps.projectDistinctVertices(0).writeAsText(args[2], WriteMode.OVERWRITE);
			    env.execute();
			    break;
			}
			
			case "9" : {
				//MATCH (m:comment) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:post)
				//RETURN n
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
				DataSet<ArrayList<Long>> result = binaryOps.joinOnAfterVertices(2, 2);
	    
				UnaryOperators unaryOps = new UnaryOperators(graph, result);
				unaryOps.projectDistinctVertices(2).writeAsText(args[2], WriteMode.OVERWRITE);
				env.execute();
				
				break;
			}
			case "10": {
				//MATCH (m:person) - [:likes] -> (n:comment)
				//RETURN m		
				ScanOperators scanOps1 = new ScanOperators(graph);
				HashSet<String> q1 = new HashSet<>();
				q1.add("person");
				DataSet<ArrayList<Long>> paths1 = scanOps1.getInitialVerticesByLabels(q1);
				
				UnaryOperators unaryOps1 = new UnaryOperators(graph, paths1);
				DataSet<ArrayList<Long>> paths2 = unaryOps1.selectOutEdgesByLabel(0, "likes", JoinHint.BROADCAST_HASH_FIRST);
				
				UnaryOperators unaryOps2 = new UnaryOperators(graph, paths2);
				HashSet<String> q2 = new HashSet<>();
				q2.add("comment");
				unaryOps2.selectVerticesByLabels(2, q2);
				
				unaryOps2.projectDistinctVertices(0).writeAsText(args[2], WriteMode.OVERWRITE);
				env.execute();
				break;
			}
			case "11": {
				ScanOperators scanOps1 = new ScanOperators(graph);
				HashSet<String> q1 = new HashSet<>();
				q1.add("person");
				DataSet<ArrayList<Long>> paths1 = scanOps1.getInitialVerticesByLabels(q1);
				
				UnaryOperators unaryOps1 = new UnaryOperators(graph, paths1);
				DataSet<ArrayList<Long>> paths2 = unaryOps1.selectOutEdgesByLabel(0, "likes", JoinHint.BROADCAST_HASH_FIRST);
				
				ScanOperators scanOps2 = new ScanOperators(graph);
				HashSet<String> q2 = new HashSet<>();
				q2.add("comment");
				DataSet<ArrayList<Long>> paths3 = scanOps2.getInitialVerticesByLabels(q2);
				
				BinaryOperators binaryOps1 = new BinaryOperators(paths2, paths3);
				DataSet<ArrayList<Long>> result = binaryOps1.joinOnAfterVertices(1, 0);
			
				UnaryOperators unaryOps2 = new UnaryOperators(graph, result);
				unaryOps2.projectDistinctVertices(0).writeAsText(args[2], WriteMode.OVERWRITE);
				env.execute();
				break;
			}
			case "12": {
				ScanOperators scanOps1 = new ScanOperators(graph);
				HashSet<String> q1 = new HashSet<>();
				q1.add("person");
				DataSet<ArrayList<Long>> paths1 = scanOps1.getInitialVerticesByLabels(q1);
				
				UnaryOperators unaryOps1 = new UnaryOperators(graph, paths1);
				DataSet<ArrayList<Long>> paths2 = unaryOps1.selectOutEdgesByLabel(0, "likes", JoinHint.BROADCAST_HASH_FIRST);
				
				ScanOperators scanOps2 = new ScanOperators(graph);
				HashSet<String> q2 = new HashSet<>();
				q2.add("comment");
				DataSet<ArrayList<Long>> paths3 = scanOps2.getInitialVerticesByLabels(q2);
				
				BinaryOperators binaryOps1 = new BinaryOperators(paths2, paths3);
				DataSet<ArrayList<Long>> result = binaryOps1.joinOnAfterVertices(1, 0);
			
				UnaryOperators unaryOps2 = new UnaryOperators(graph, result);
				unaryOps2.projectDistinctVertices(0).writeAsText(args[2], WriteMode.OVERWRITE);
				env.execute();
				break;
			}
			case "13": {
				ScanOperators scanOps1 = new ScanOperators(graph);
				HashSet<String> q1 = new HashSet<>();
				q1.add("comment");
				DataSet<ArrayList<Long>> paths1 = scanOps1.getInitialVerticesByLabels(q1);
				
				ScanOperators scanOps2 = new ScanOperators(graph);
				HashSet<String> q2 = new HashSet<>();
				q2.add("person");
				DataSet<ArrayList<Long>> paths2 = scanOps2.getInitialVerticesByLabels(q2);

				ScanOperators scanOps3 = new ScanOperators(graph);
				HashSet<String> q3 = new HashSet<>();
				q3.add("post");
				DataSet<ArrayList<Long>> paths3 = scanOps3.getInitialVerticesByLabels(q3);
				
				UnaryOperators unaryOps1 = new UnaryOperators(graph, paths1);
				DataSet<ArrayList<Long>> paths4 = unaryOps1.selectOutEdgesByLabel(0, "hasCreator", JoinHint.BROADCAST_HASH_FIRST);

				BinaryOperators binaryOps1 = new BinaryOperators(paths4, paths2);
				DataSet<ArrayList<Long>> paths5 = binaryOps1.joinOnAfterVertices(2, 0);
								
				UnaryOperators unaryOps2 = new UnaryOperators(graph, paths5);
				DataSet<ArrayList<Long>> paths6 = unaryOps2.selectInEdgesByLabel(2, "hasCreator", JoinHint.BROADCAST_HASH_FIRST);
				
				BinaryOperators binaryOps2 = new BinaryOperators(paths6, paths3);
				DataSet<ArrayList<Long>> paths7 = binaryOps2.joinOnAfterVertices(4, 0);
				
				UnaryOperators unaryOps3 = new UnaryOperators(graph, paths7);
				unaryOps3.projectDistinctVertices(2).writeAsText(args[2], WriteMode.OVERWRITE);
				env.execute();
				break;
				
			}
			case "14": {
				ScanOperators scanOps1 = new ScanOperators(graph);
				HashSet<String> q1 = new HashSet<>();
				q1.add("comment");
				DataSet<ArrayList<Long>> paths1 = scanOps1.getInitialVerticesByLabels(q1);
				
				ScanOperators scanOps2 = new ScanOperators(graph);
				HashSet<String> q2 = new HashSet<>();
				q2.add("person");
				DataSet<ArrayList<Long>> paths2 = scanOps2.getInitialVerticesByLabels(q2);

				ScanOperators scanOps3 = new ScanOperators(graph);
				HashSet<String> q3 = new HashSet<>();
				q3.add("post");
				DataSet<ArrayList<Long>> paths3 = scanOps3.getInitialVerticesByLabels(q3);
				
				UnaryOperators unaryOps1 = new UnaryOperators(graph, paths1);
				DataSet<ArrayList<Long>> paths4 = unaryOps1.selectOutEdgesByLabel(0, "hasCreator", JoinHint.BROADCAST_HASH_FIRST);

				UnaryOperators unaryOps2 = new UnaryOperators(graph, paths3);
				DataSet<ArrayList<Long>> paths5 = unaryOps2.selectOutEdgesByLabel(0, "hasCreator", JoinHint.BROADCAST_HASH_FIRST);

				BinaryOperators binaryOps1 = new BinaryOperators(paths4, paths5);
				DataSet<ArrayList<Long>> paths6 = binaryOps1.joinOnAfterVertices(2, 2);
				
				BinaryOperators binaryOps2 = new BinaryOperators(paths6, paths2);
				DataSet<ArrayList<Long>> paths7 = binaryOps2.joinOnAfterVertices(2, 0);
				
				UnaryOperators unaryOps3 = new UnaryOperators(graph, paths7);
				unaryOps3.projectDistinctVertices(2).writeAsText(args[2], WriteMode.OVERWRITE);
				env.execute();
				break;
				
			}
		}
	
	    
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
			String propString = vertexFromFile.f2.substring(1, vertexFromFile.f2.length()-1);
			String[] fields = propString.split(", ");
			String lastk = null;
			for (String f: fields) {
				String[] kv = f.split("=", 2);
				if (kv.length == 1) {
					// Continuation of last field
					if (lastk == null) {
						throw new Exception("Bad property string " + propString);
					}
					properties.put(lastk, properties.get(lastk) + ", " + kv[0]);
				} else {
					// New field
					properties.put(kv[0], kv[1]);
					lastk = kv[0];
				}
			}
			vertex.setProps(properties);
			
			return vertex;
			
		}
		
	}
	
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
