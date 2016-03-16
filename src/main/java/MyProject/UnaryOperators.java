package MyProject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashSet;

import MyProject.DataStructures.*;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

public class UnaryOperators {
	
	private GraphExtended<Long, ArrayList<String>, HashMap<String, String>, String,
	  String, HashMap<String, String>> graph;
	private DataSet<ArrayList<Tuple2<String, Long>>> vertexAndEdgeIds;
	
	/*get the input graph, current columnNumber and the vertex and edges Ids*/
	public UnaryOperators(GraphExtended<Long, ArrayList<String>, HashMap<String, String>, String,
			  String, HashMap<String, String>> g,
			  DataSet<ArrayList<Tuple2<String, Long>>> verticesAndEdges) {
		this.graph = g;
		this.vertexAndEdgeIds = verticesAndEdges;
	}
	
	/*No requirements on the current vertices*/
	public DataSet<ArrayList<Tuple2<String, Long>>> selectOnVertices() {
		return vertexAndEdgeIds;
	}
	
	public DataSet<ArrayList<Tuple2<String, Long>>> selectOnVerticesByLabels(ArrayList<String> labels, final int colNum){
		DataSet<ArrayList<Tuple2<String, Long>>> selectedResults = vertexAndEdgeIds
			.groupBy(new KeySelectorOfVertices(colNum))
			 .getDataSet()
			 .join(graph.getVertices())
			 .where(new KeySelectorOfVertices(colNum))
			 .equalTo(0)
			 .with(new JoinAndFilterVerticesByLabels(labels));
		return selectedResults;
	}
	
	private static class KeySelectorOfVertices implements 
	KeySelector<ArrayList<Tuple2<String, Long>>, Long> {
		private static final long serialVersionUID = 1L;
		private int colNum = 0;

		KeySelectorOfVertices(int columnNum) {this.colNum = columnNum;}
		@Override
		public Long getKey(ArrayList<Tuple2<String, Long>> row)
				throws Exception {
			return row.get(colNum).f1;
		}
	}
	private static class JoinAndFilterVerticesByLabels implements
			FlatJoinFunction<ArrayList<Tuple2<String, Long>>, VertexExtended<Long, ArrayList<String>,
			HashMap<String, String>>, ArrayList<Tuple2<String, Long>>> {

		private static final long serialVersionUID = 1L;
		private ArrayList<String> labels = new ArrayList<>();
		JoinAndFilterVerticesByLabels(ArrayList<String> labelList) { this.labels = labelList;}
		@Override
		public void join(
				ArrayList<Tuple2<String, Long>> edgesAndVertices,
				VertexExtended<Long, ArrayList<String>, HashMap<String, String>> vertex,
				Collector<ArrayList<Tuple2<String, Long>>> outEdgesAndVertices)
				throws Exception {
					if(vertex.getLabels().containsAll(labels))
						outEdgesAndVertices.collect(edgesAndVertices);
		}	
	};
		

	/*public void selectOnVertices(final HashMap<String, String> props){
		DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> verticesSelectedByProps = 
			
			graph.getVertices()
			
				 .filter(
					new FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>(){
						
						private static final long serialVersionUID = 1L;
						@Override
						public boolean filter(
							VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex)
									throws Exception {
							HashMap<String, String> propsOfVertex = vertex.f2;
							if(props.isEmpty())
								return true;
							for(Map.Entry<String, String> entry : propsOfVertex.entrySet()) {
								if(propsOfVertex.get(entry.getKey()) == null || 
										!propsOfVertex.get(entry.getKey()).equals(entry.getValue()))
									return false;
							}
							return true;
						}
					});
	}*/
	public void selectOnVertices(String label, HashMap<String, String> props){
		
	}
	
	public void selectOnVertices(String[] label){
		
	}

	
	public void selectOnEdges(){
		
	}
	
	public static void projectOnVertices(){
	
	}
	
	public static void projectOnEdges(){
		
	}

	
		
}
