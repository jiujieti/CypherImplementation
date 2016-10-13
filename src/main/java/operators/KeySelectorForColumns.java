package operators;
import java.util.ArrayList;

import org.apache.flink.api.java.functions.KeySelector;

/*
 * key selector is used for determining the position of a vertex or an edge in a path ArrayList
 * 
 * */
@SuppressWarnings("serial")
public class KeySelectorForColumns implements KeySelector<ArrayList<Long>, Long> {
	
	private int col = 0;
	
	KeySelectorForColumns(int column) {this.col = column;}

	@Override
	public Long getKey(ArrayList<Long> row) throws Exception {
		return row.get(col);
	}
}

