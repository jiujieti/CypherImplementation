package queryplan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import operators.BinaryOperators;
import operators.ScanOperators;
import operators.UnaryOperators;
import operators.booleanExpressions.AND;
import operators.booleanExpressions.comparisons.LabelComparisonForEdges;
import operators.booleanExpressions.comparisons.LabelComparisonForVertices;
 
import operators.booleanExpressions.comparisons.PropertyFilterForVertices;
import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;
import operators.datastructures.VertexExtended;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

import queryplan.querygraph.QueryEdge;
import queryplan.querygraph.QueryGraph;
import queryplan.querygraph.QueryGraphComponent;
import queryplan.querygraph.QueryVertex;
@SuppressWarnings("unchecked")
public class QueryPlanner {
	QueryGraph query;
	GraphExtended<Long, HashSet<String>, HashMap<String, String>, 
    	Long, String, HashMap<String, String>> graph;
	HashMap<String, Tuple2<Long, Double>> verticesStats;
	HashMap<String, Tuple2<Long, Double>> edgesStats;
	
	public QueryPlanner(QueryGraph q, GraphExtended<Long, HashSet<String>, HashMap<String, String>, 
		      Long, String, HashMap<String, String>> g, HashMap<String, Tuple2<Long, Double>> vs,
		      HashMap<String, Tuple2<Long, Double>> es) {
		query = q;
		graph = g;
		verticesStats = vs;
		edgesStats = es;
	}
	
	public void genQueryPlan() throws Exception {
		//traverse each query vertex and generate a initial component
		for(QueryVertex qv: query.getQueryVertices()){
			double est = qv.getPrio();
			ScanOperators s = new ScanOperators(graph);
			
			FilterFunction vf;
			FilterFunction newvf;
			vf = new LabelComparisonForVertices(qv.getLabel());
			if(!qv.getProps().isEmpty()) {
				HashMap<String, Tuple2<String, Double>> props = (HashMap<String, Tuple2<String, Double>>) qv.getProps().clone();
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
				double estEdge = e.getPrio() + estSrc + estTar;
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
				HashMap<String, Tuple2<String, Double>> props = (HashMap<String, Tuple2<String, Double>>) e.getProps().clone();
				for(String k: props.keySet()){
					newef =  new PropertyFilterForVertices(k, props.get(k).f0, props.get(k).f1);
					ef = new AND<EdgeExtended<Long, Long, String, HashMap<String, String>>>(ef, newef);
				}
			} 
			if(e.getSourceVertex().getComponent().getEst() <= e.getTargetVertex().getComponent().getEst()) {
				System.out.println("HAHAHAHA " + e.getSourceVertex().getLabel() + " " + e.getLabel() + " " + e.getTargetVertex().getLabel() + " out");
				UnaryOperators u = new UnaryOperators(graph, e.getSourceVertex().getComponent().getData()) ;
				int firstCol = e.getSourceVertex().getComponent().getVertexIndex(e.getSourceVertex());
				//paths = u.selectOutEdgesByLabel(firstCol, e.getLabel(), JoinHint.BROADCAST_HASH_FIRST);
				paths = u.selectOutEdgesByBooleanExpressions(firstCol, ef, JoinHint.BROADCAST_HASH_SECOND);
				
				leftColumns = e.getSourceVertex().getComponent().getColumns();
				
				BinaryOperators b = new BinaryOperators(paths, e.getTargetVertex().getComponent().getData());
				int secondCol = e.getTargetVertex().getComponent().getVertexIndex(e.getTargetVertex());
				joinedPaths = b.joinOnAfterVertices(leftColumns.size() + 1, secondCol);
				
				rightColumns = (ArrayList<Object>) e.getTargetVertex().getComponent().getColumns().clone();
				rightColumns.remove(secondCol);
				rightColumns.add(0, e.getTargetVertex());
			}
			else {
				System.out.println("HAHAHAHA " + e.getSourceVertex().getLabel() + " " + e.getLabel() + " " + e.getTargetVertex().getLabel() + " in");
				UnaryOperators u = new UnaryOperators(graph, e.getTargetVertex().getComponent().getData()) ;
				int firstCol = e.getTargetVertex().getComponent().getVertexIndex(e.getTargetVertex());
				//paths = u.selectInEdgesByLabel(firstCol, e.getLabel(), JoinHint.BROADCAST_HASH_FIRST);
				paths = u.selectOutEdgesByBooleanExpressions(firstCol, ef, JoinHint.BROADCAST_HASH_SECOND);
				
				
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
		/*
		for (Object o: query.getQueryVertices()[0].getComponent().getColumns()) {
			if (o.getClass() == QueryVertex.class) {
				QueryVertex qv = (QueryVertex) o;
				System.out.println(qv.getLabel());
			} else {
				QueryEdge qe = (QueryEdge) o;
				System.out.println(qe.getLabel());
			}
		}
		*/
	}
	

}
