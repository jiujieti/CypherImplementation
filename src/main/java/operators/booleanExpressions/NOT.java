package operators.booleanExpressions;

import org.apache.flink.api.common.functions.FilterFunction;

@SuppressWarnings("serial")
public class NOT<T> implements FilterFunction<T> {
	
	private FilterFunction<T> ft;

	public NOT(FilterFunction<T> ft) {this.ft = ft;}
	
	@Override
	public boolean filter(T element) throws Exception {
		return !ft.filter(element);
	}	
}
