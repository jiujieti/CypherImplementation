package MyProject;

import java.util.ArrayList;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

@SuppressWarnings("serial")
class KeySelectorForVertices implements KeySelector<ArrayList<Tuple2<String, Long>>, Long> {
	private int col = 0;
	
	KeySelectorForVertices(int column) {this.col = column;}
	
	@Override
	public Long getKey(ArrayList<Tuple2<String, Long>> row)
			throws Exception {
		return row.get(col).f1;
	}
}
