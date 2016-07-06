package queryplan.querygraph;
import java.util.ArrayList;

import org.apache.flink.api.java.DataSet;
public class QueryGraphComponent {
	double est;
	DataSet<ArrayList<Long>> data;
	ArrayList<Object> columns;
	
	
	public QueryGraphComponent(double e, DataSet<ArrayList<Long>> d, ArrayList<Object> cols) {
		est = e;
		data = d;
		columns = cols;
	}
	
	public double getEst() {
		return est;
	} 
	
	public DataSet<ArrayList<Long>> getData() {
		return data;
	}
	
	public ArrayList<Object> getColumns() {
		return columns;
	}
	
	public int getVertexIndex(QueryVertex qv) {
		for(int i = 0; i < columns.size(); i++) {
			if(columns.get(i) == qv) return i;
		}
		return -1;
	}
}