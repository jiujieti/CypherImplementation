package queryplan;

import java.util.HashMap;
import java.util.HashSet;

import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;
import operators.datastructures.VertexExtended;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem.WriteMode;




import queryplan.querygraph.QueryEdge;
import queryplan.querygraph.QueryGraph;
import queryplan.querygraph.QueryVertex;
@SuppressWarnings("serial")
public class QueryGeneratorTest {
	
	public static void main(String[] args) throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		//String dir = "C:/Users/s146508/Desktop/ubuntu/5kPerson/";
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
		
		StatisticsTransformation sts = new StatisticsTransformation(dir, env);
		HashMap<String, Tuple2<Long, Double>> vstat = sts.getVerticesStatistics();
		HashMap<String, Tuple2<Long, Double>> estat = sts.getEdgesStatistics();
		GraphExtended<Long, HashSet<String>, HashMap<String, String>, 
	      Long, String, HashMap<String, String>> graph = GraphExtended.fromDataSet(vertices, edges, env);
				
		switch(args[1]) {
			case "0" : {
				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) - [:hasTag] -> (k:Tag)
				//WHERE l.length >= 150
				//RETURN n
				HashMap<String, Tuple2<String, String>> commentProps = new HashMap<>();
				
				//browserUsed=Chrome
				commentProps.put("length", new Tuple2<String, String>(">=", "150")); 
				
				QueryVertex a = new QueryVertex("post",  new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex b = new QueryVertex("person",  new HashMap<String, Tuple2<String, String>>(), true);
				QueryVertex c = new QueryVertex("comment", commentProps, false);
				QueryVertex d = new QueryVertex("tag",  new HashMap<String, Tuple2<String, String>>(), false);
				
				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge cd = new QueryEdge(c, d, "hasTag", new HashMap<String, Tuple2<String, String>>());
				
				QueryVertex[] vs = {a, b, c, d};
				QueryEdge[] es = {ab, cb, cd};
				
				QueryGraph g = new QueryGraph(vs, es);

				QueryPlanGenerator pg = new QueryPlanGenerator(g, graph, vstat, estat);
				DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.generateQueryPlan();
				res.writeAsText(args[2], WriteMode.OVERWRITE);	
				env.execute();
				break;
			} 
			
			case "1" : {
				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) - [:hasTag] -> (k:Tag)
				//WHERE n.lastName = 'Yang'
				//RETURN n

				HashMap<String, Tuple2<String, String>> personProps = new HashMap<>();
				personProps.put("lastName", new Tuple2<String, String>("eq", "Yang")); 
				
				QueryVertex a = new QueryVertex("post", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex b = new QueryVertex("person",  personProps, true);
				QueryVertex c = new QueryVertex("comment", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex d = new QueryVertex("tag", new HashMap<String, Tuple2<String, String>>(), false);
				
				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge cd = new QueryEdge(c, d, "hasTag", new HashMap<String, Tuple2<String, String>>());
				
				QueryVertex[] vs = {a, b, c, d};
				QueryEdge[] es = {ab, cb, cd};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				QueryPlanGenerator pg = new QueryPlanGenerator(g, graph, vstat, estat);
				DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.generateQueryPlan();
				res.writeAsText(args[2], WriteMode.OVERWRITE);
				env.execute();
				break;
			}
			
			case "2" : {
				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) - [:hasTag] -> (k:Tag)
				//WHERE n.lastName = 'Yang' AND n.browserUsed = 'Safari'
				//RETURN n
				
				HashMap<String, Tuple2<String, String>> personProps = new HashMap<>();
				personProps.put("lastName", new Tuple2<String, String>("eq", "Yang")); 
				personProps.put("browserUsed", new Tuple2<String, String>("eq", "Safari"));
				
				QueryVertex a = new QueryVertex("post", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex b = new QueryVertex("person",  personProps, true);
				QueryVertex c = new QueryVertex("comment", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex d = new QueryVertex("tag", new HashMap<String, Tuple2<String, String>>(), false);
				
				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge cd = new QueryEdge(c, d, "hasTag", new HashMap<String, Tuple2<String, String>>());
				
				QueryVertex[] vs = {a, b, c, d};
				QueryEdge[] es = {ab, cb, cd};
				
				QueryGraph g = new QueryGraph(vs, es);

				QueryPlanGenerator pg = new QueryPlanGenerator(g, graph, vstat, estat);
				DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.generateQueryPlan();
				res.writeAsText(args[2], WriteMode.OVERWRITE);
				env.execute();
				break;
			}
			
			case "3" : {
				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
				//WHERE (n) - [:studyAt] -> (o:organisation) AND
				//		(n) - [:isLocatedIn] -> (p:place) AND
				//      l.length >= 150
				//RETURN n
				HashMap<String, Tuple2<String, String>> commentProps = new HashMap<>();
				commentProps.put("length", new Tuple2<String, String>(">=", "150")); 
				QueryVertex a = new QueryVertex("post", new HashMap<String, Tuple2<String, String>>() , false);
				QueryVertex b = new QueryVertex("person", new HashMap<String, Tuple2<String, String>>(), true);
				QueryVertex c = new QueryVertex("comment", commentProps, false);
				QueryVertex d = new QueryVertex("organisation", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex e = new QueryVertex("place", new HashMap<String, Tuple2<String, String>>(), false);
				
				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Tuple2<String, String>>());
				QueryEdge be = new QueryEdge(b, e, "isLocatedIn", new HashMap<String, Tuple2<String, String>>());
				
				QueryVertex[] vs = {a, b, c, d, e};
				QueryEdge[] es = {ab, cb, bd, be};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				QueryPlanGenerator pg = new QueryPlanGenerator(g, graph, vstat, estat);
				DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.generateQueryPlan();
				res.writeAsText(args[2], WriteMode.OVERWRITE);
				env.execute();
				break;
				
			}
			case "4" : {
				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
				//WHERE (n) - [:studyAt] -> (o:organisation) AND
				//		(n) - [:isLocatedIn] -> (p:place) AND
				// 		n.lastName = 'Yang'
				//RETURN n
				
				HashMap<String, Tuple2<String, String>> personProps = new HashMap<>();
				personProps.put("lastName", new Tuple2<String, String>("eq", "Yang")); 
				
				QueryVertex a = new QueryVertex("post", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex b = new QueryVertex("person", personProps, true);
				QueryVertex c = new QueryVertex("comment", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex d = new QueryVertex("organisation", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex e = new QueryVertex("place", new HashMap<String, Tuple2<String, String>>(), false);
				
				
				
				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Tuple2<String, String>>());
				QueryEdge be = new QueryEdge(b, e, "isLocatedIn", new HashMap<String, Tuple2<String, String>>());
				
				QueryVertex[] vs = {a, b, c, d, e};
				QueryEdge[] es = {ab, cb, bd, be};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				QueryPlanGenerator pg = new QueryPlanGenerator(g, graph, vstat, estat);
				DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.generateQueryPlan();
				res.writeAsText(args[2], WriteMode.OVERWRITE);
				env.execute();
				break;
				
			}
			case "5" : {
				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
				//WHERE (n) - [:studyAt] -> (o:organisation) AND
				//		(n) - [:isLocatedIn] -> (p:place) AND
				// 		o.type = 'company'
				//RETURN n
				
				HashMap<String, Tuple2<String, String>> orgProps = new HashMap<>();
				orgProps.put("type", new Tuple2<String, String>("eq", "company")); 
				
				QueryVertex a = new QueryVertex("post", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex b = new QueryVertex("person", new HashMap<String, Tuple2<String, String>>(), true);
				QueryVertex c = new QueryVertex("comment", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex d = new QueryVertex("organisation", orgProps, false);
				QueryVertex e = new QueryVertex("place", new HashMap<String, Tuple2<String, String>>(), false);
				
				
				
				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Tuple2<String, String>>());
				QueryEdge be = new QueryEdge(b, e, "isLocatedIn", new HashMap<String, Tuple2<String, String>>());
				
				QueryVertex[] vs = {a, b, c, d, e};
				QueryEdge[] es = {ab, cb, bd, be};
				
				QueryGraph g = new QueryGraph(vs, es);
				QueryPlanGenerator pg = new QueryPlanGenerator(g, graph, vstat, estat);
				DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.generateQueryPlan();
				res.writeAsText(args[2], WriteMode.OVERWRITE);
				env.execute();
				break;
				
			}
			case "6" : {
				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
				//WHERE (n) - [:studyAt] -> (o:organisation) AND
				//		(l) - [:hasTag] -> (t:tag) AND
				//		(l) - [:isLocatedIn] -> (p:place) AND 
				// 		l.length >= 175
				//RETURN n
				
				HashMap<String, Tuple2<String, String>> commentProps = new HashMap<>();
				commentProps.put("length", new Tuple2<String, String>(">=", "175")); 
				
				QueryVertex a = new QueryVertex("post", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex b = new QueryVertex("person", new HashMap<String, Tuple2<String, String>>(), true);
				QueryVertex c = new QueryVertex("comment", commentProps, false);
				QueryVertex d = new QueryVertex("organisation",new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex e = new QueryVertex("tag", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex f = new QueryVertex("place", new HashMap<String, Tuple2<String, String>>(), false);
				
				
				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Tuple2<String, String>>());
				QueryEdge ce = new QueryEdge(c, e, "hasTag", new HashMap<String, Tuple2<String, String>>());
				QueryEdge cf = new QueryEdge(c, f, "isLocatedIn", new HashMap<String, Tuple2<String, String>>());
				
				
				QueryVertex[] vs = {a, b, c, d, e, f};
				QueryEdge[] es = {ab, cb, bd, ce, cf};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				QueryPlanGenerator pg = new QueryPlanGenerator(g, graph, vstat, estat);
				DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.generateQueryPlan();
				res.writeAsText(args[2], WriteMode.OVERWRITE);
				env.execute();
				break;
				
			}

			case "7" : {
				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
				//WHERE (n) - [:studyAt] -> (o:organisation) AND
				//		(l) - [:hasTag] -> (t:tag) AND
				//		(l) - [:isLocatedIn] -> (p:place) AND 
				// 		n.lastName = 'Yang'
				//RETURN n
				
				HashMap<String, Tuple2<String, String>> personProps = new HashMap<>();
				personProps.put("lastName", new Tuple2<String, String>("eq", "Yang")); 
				
				QueryVertex a = new QueryVertex("post", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex b = new QueryVertex("person", personProps, true);
				QueryVertex c = new QueryVertex("comment", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex d = new QueryVertex("organisation",new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex e = new QueryVertex("tag", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex f = new QueryVertex("place", new HashMap<String, Tuple2<String, String>>(), false);
				
				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Tuple2<String, String>>());
				QueryEdge ce = new QueryEdge(c, e, "hasTag", new HashMap<String, Tuple2<String, String>>());
				QueryEdge cf = new QueryEdge(c, f, "isLocatedIn", new HashMap<String, Tuple2<String, String>>());
				
				
				QueryVertex[] vs = {a, b, c, d, e, f};
				QueryEdge[] es = {ab, cb, bd, ce, cf};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				QueryPlanGenerator pg = new QueryPlanGenerator(g, graph, vstat, estat);
				DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.generateQueryPlan();
				res.writeAsText(args[2], WriteMode.OVERWRITE);
				env.execute();
				break;
				
			}

			case "8" : {
				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
				//WHERE (n) - [:studyAt] -> (o:organisation) AND
				//		(l) - [:hasTag] -> (t:tag) AND
				//		(l) - [:isLocatedIn] -> (p:place) AND 
				// 		o.type = 'company'
				//RETURN n
				
				HashMap<String, Tuple2<String, String>> orgProps = new HashMap<>();
				orgProps.put("type", new Tuple2<String, String>("eq", "company")); 
				
				QueryVertex a = new QueryVertex("post", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex b = new QueryVertex("person", new HashMap<String, Tuple2<String, String>>(), true);
				QueryVertex c = new QueryVertex("comment", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex d = new QueryVertex("organisation",orgProps, false);
				QueryVertex e = new QueryVertex("tag", new HashMap<String, Tuple2<String, String>>(), false);
				QueryVertex f = new QueryVertex("place", new HashMap<String, Tuple2<String, String>>(), false);
				
				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Tuple2<String, String>>());
				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Tuple2<String, String>>());
				QueryEdge ce = new QueryEdge(c, e, "hasTag", new HashMap<String, Tuple2<String, String>>());
				QueryEdge cf = new QueryEdge(c, f, "isLocatedIn", new HashMap<String, Tuple2<String, String>>());
				
				
				QueryVertex[] vs = {a, b, c, d, e, f};
				QueryEdge[] es = {ab, cb, bd, ce, cf};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				QueryPlanGenerator pg = new QueryPlanGenerator(g, graph, vstat, estat);
				DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.generateQueryPlan();
				res.writeAsText(args[2], WriteMode.OVERWRITE);
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
			/*String pattern = "[^=]+=([^= ]*( |$))*";
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
			}*/
			String propString = vertexFromFile.f2.substring(1, vertexFromFile.f2.length()-1);
			String[] fields = propString.split(", ");
			String lastk = null;
			for (String f: fields) {
				String[] kv = f.split("=", 2);
				if (kv.length == 1) {
					// Continuation of last field
					if (lastk == null) {
						throw new Exception("bad property string " + propString);
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
