package queryplan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;








import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

import queryplan.querygraph.*;
import operators.ScanOperators;
import operators.UnaryOperators;
import operators.BinaryOperators;
import operators.booleanExpressions.AND;
import operators.booleanExpressions.comparisons.LabelComparisonForEdges;
import operators.booleanExpressions.comparisons.LabelComparisonForVertices;
import operators.booleanExpressions.comparisons.PropertyFilterForEdges;
import operators.booleanExpressions.comparisons.PropertyFilterForVertices;
import operators.datastructures.*;

@SuppressWarnings("unchecked")
/*A query plan generator which only utilizes the statistics */
public class QueryPlanGenerator {
	QueryGraph query;
	GraphExtended<Long, HashSet<String>, HashMap<String, String>, 
    	Long, String, HashMap<String, String>> graph;
	HashMap<String, Tuple2<Long, Double>> verticesStats;
	HashMap<String, Tuple2<Long, Double>> edgesStats;
	
	public QueryPlanGenerator(QueryGraph q, GraphExtended<Long, HashSet<String>, HashMap<String, String>, 
		      Long, String, HashMap<String, String>> g, HashMap<String, Tuple2<Long, Double>> vs,
		      HashMap<String, Tuple2<Long, Double>> es) {
		query = q;
		graph = g;
		verticesStats = vs;
		edgesStats = es;
	}
	
	
	public DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> generateQueryPlan() throws Exception {
		//traverse each query vertex and generate a initial component
		for(QueryVertex qv: query.getQueryVertices()){
			double est = verticesStats.get(qv.getLabel()).f1;
			ScanOperators s = new ScanOperators(graph);
			
			FilterFunction vf;
			FilterFunction newvf;
			vf = new LabelComparisonForVertices(qv.getLabel());
			if(!qv.getProps().isEmpty()) {
				HashMap<String, Tuple2<String, String>> props = (HashMap<String, Tuple2<String, String>>) qv.getProps().clone();
				for(String k: props.keySet()){
					newvf =  new PropertyFilterForVertices(k, props.get(k).f0, props.get(k).f1);
					vf = new AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>
						(vf, newvf);
				}
			} 
			
			DataSet<ArrayList<Long>> paths = s.getInitialVerticesByBooleanExpressions(vf);
			
			ArrayList<Object> cols = new ArrayList<>();
			cols.add(qv);
			qv.setComponent(new QueryGraphComponent(est, paths, cols));
		}
		
		ArrayList<QueryEdge> edges= new ArrayList<> (Arrays.asList(query.getQueryEdges()));
		//System.out.println("LALALA");
		while (!edges.isEmpty()) {
			//traverse statistics, selects the edge with lowest cost
			double minEst = Double.MAX_VALUE;
			QueryEdge e = edges.get(0);
			for(QueryEdge cand: edges){
				double estSrc = cand.getSourceVertex().getComponent().getEst();
				double estTar = cand.getTargetVertex().getComponent().getEst();
				double estEdge = edgesStats.get(cand.getLabel()).f0 * estSrc * estTar;
				if (minEst > estEdge) {
					minEst = estEdge;
					e = cand;
				}
			}
			edges.remove(e);
			
			DataSet<ArrayList<Long>> paths, joinedPaths;
			ArrayList<Object> leftColumns, rightColumns;
			FilterFunction ef;
			FilterFunction newef;
			ef = new LabelComparisonForEdges(e.getLabel());
			if(!e.getProps().isEmpty()) {
				HashMap<String, Tuple2<String, String>> props = (HashMap<String, Tuple2<String, String>>) e.getProps().clone();
				for(String k: props.keySet()){
					newef =  new PropertyFilterForEdges(k, props.get(k).f0, props.get(k).f1);
					ef = new AND<EdgeExtended<Long, Long, String, HashMap<String, String>>>(ef, newef);
				}
			} 
			
			/*if(e.getSourceVertex().getComponent().getEst() <= e.getTargetVertex().getComponent().getEst()) {
				//System.out.println("HAHAHAHA " + e.getSourceVertex().getLabel() + " " + e.getLabel() + " " + e.getTargetVertex().getLabel() + " out");
				UnaryOperators u = new UnaryOperators(graph, e.getSourceVertex().getComponent().getData()) ;
				int firstCol = e.getSourceVertex().getComponent().getVertexIndex(e.getSourceVertex());
				paths = u.selectOutEdgesByLabel(firstCol, e.getLabel(), JoinHint.BROADCAST_HASH_FIRST);
				
				leftColumns = e.getSourceVertex().getComponent().getColumns();
				
				BinaryOperators b = new BinaryOperators(paths, e.getTargetVertex().getComponent().getData());
				int secondCol = e.getTargetVertex().getComponent().getVertexIndex(e.getTargetVertex());
				joinedPaths = b.joinOnAfterVertices(leftColumns.size() + 1, secondCol);
				
				rightColumns = (ArrayList<Object>) e.getTargetVertex().getComponent().getColumns().clone();
				rightColumns.remove(secondCol);
				rightColumns.add(0, e.getTargetVertex());
			}
			else {
				//System.out.println("HAHAHAHA " + e.getSourceVertex().getLabel() + " " + e.getLabel() + " " + e.getTargetVertex().getLabel() + " in");
				UnaryOperators u = new UnaryOperators(graph, e.getTargetVertex().getComponent().getData()) ;
				int firstCol = e.getTargetVertex().getComponent().getVertexIndex(e.getTargetVertex());
				paths = u.selectInEdgesByLabel(firstCol, e.getLabel(), JoinHint.BROADCAST_HASH_FIRST);

				leftColumns = e.getTargetVertex().getComponent().getColumns();
				
				BinaryOperators b = new BinaryOperators(paths, e.getSourceVertex().getComponent().getData());
				int secondCol = e.getSourceVertex().getComponent().getVertexIndex(e.getSourceVertex());
				joinedPaths = b.joinOnAfterVertices(leftColumns.size()+1, secondCol);
				
				rightColumns = (ArrayList<Object>) e.getSourceVertex().getComponent().getColumns().clone();
				rightColumns.remove(secondCol);
				rightColumns.add(0, e.getSourceVertex());
			}
			ArrayList<Object> columns = new ArrayList<>();
			columns.addAll(leftColumns);
			columns.add(e);
			columns.addAll(rightColumns);

			//System.out.println("HAHAHA " + paths.count() + ", " + joinedPaths.count());

			double est = minEst / verticesStats.get("vertices").f0;
			//DataSet<Long> est =  e.getSourceVertex().getComponent().getData().map(new CountDataSize()).aggregate(Aggregations.SUM, 0);
			QueryGraphComponent gc = new QueryGraphComponent(est, joinedPaths, columns);
			
			for(Object o: columns) { 
				if(o.getClass() == QueryVertex.class) {
					QueryVertex qv = (QueryVertex) o;
					qv.setComponent(gc);
				}
			}
		}
		query.getQueryVertices()[0].getComponent().getData().count();
	
	}*/
			if(e.getSourceVertex().getComponent().getEst() >= e.getTargetVertex().getComponent().getEst()) {
				//System.out.println("LALALA " + e.getSourceVertex().getLabel() + " " + e.getLabel() + " " + e.getTargetVertex().getLabel() + "OUT");
				UnaryOperators u = new UnaryOperators(graph, e.getSourceVertex().getComponent().getData()) ;
				int firstCol = e.getSourceVertex().getComponent().getVertexIndex(e.getSourceVertex());
				
				paths = u.selectOutEdgesByBooleanExpressions(firstCol, ef, JoinHint.BROADCAST_HASH_FIRST);
				
				leftColumns = e.getSourceVertex().getComponent().getColumns();
				
				BinaryOperators b = new BinaryOperators(paths, e.getTargetVertex().getComponent().getData());
				int secondCol = e.getTargetVertex().getComponent().getVertexIndex(e.getTargetVertex());
				joinedPaths = b.joinOnAfterVertices(leftColumns.size() + 1, secondCol);
				
				rightColumns = (ArrayList<Object>) e.getTargetVertex().getComponent().getColumns().clone();
				rightColumns.remove(secondCol);
				rightColumns.add(0, e.getTargetVertex());
				//compEst = e.getSourceVertex().getComponent().getEst();
			}
			
			else {
				//System.out.println("LALALA " + e.getSourceVertex().getLabel() + " " + e.getLabel() + " " + e.getTargetVertex().getLabel() + "IN");
				UnaryOperators u = new UnaryOperators(graph, e.getTargetVertex().getComponent().getData()) ;
				int firstCol = e.getTargetVertex().getComponent().getVertexIndex(e.getTargetVertex());
				//paths = u.selectInEdgesByLabel(firstCol, e.getLabel(), JoinHint.BROADCAST_HASH_FIRST);
				paths = u.selectInEdgesByBooleanExpressions(firstCol, ef, JoinHint.BROADCAST_HASH_FIRST);
					
				leftColumns = e.getTargetVertex().getComponent().getColumns();
				
				BinaryOperators b = new BinaryOperators(paths, e.getSourceVertex().getComponent().getData());
				int secondCol = e.getSourceVertex().getComponent().getVertexIndex(e.getSourceVertex());
				joinedPaths = b.joinOnAfterVertices(leftColumns.size() + 1, secondCol);
				
				rightColumns = (ArrayList<Object>) e.getSourceVertex().getComponent().getColumns().clone();
				rightColumns.remove(secondCol);
				rightColumns.add(0, e.getSourceVertex());
				//compEst = e.getTargetVertex().getComponent().getEst();
				//System.out.println("TAT " + e.getTargetVertex().getComponent().getData().count());
						
			}
			ArrayList<Object> columns = new ArrayList<>();
			columns.addAll(leftColumns);
			columns.add(e);
			columns.addAll(rightColumns);

			double est = minEst / verticesStats.get("vertices").f0;
			//DataSet<Long> est =  e.getSourceVertex().getComponent().getData().map(new CountDataSize()).aggregate(Aggregations.SUM, 0);
			QueryGraphComponent gc = new QueryGraphComponent(est, joinedPaths, columns);
			
			for(Object o: columns) { 
				if(o.getClass() == QueryVertex.class) {
					QueryVertex qv = (QueryVertex) o;
					qv.setComponent(gc);
				}
			}
			
		}
		//where to collect outputs
		for(QueryVertex qv: query.getQueryVertices()) {
			if(qv.isOutput()) {
				UnaryOperators u = new UnaryOperators(graph, qv.getComponent().getData());
				return u.projectDistinctVertices(qv.getComponent().getVertexIndex(qv));					
			}
		}
		return null;
	}
	


}
