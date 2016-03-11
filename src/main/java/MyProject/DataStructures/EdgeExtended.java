package MyProject.DataStructures;

import org.apache.flink.api.java.tuple.Tuple4;
import java.util.HashMap;
/**
 * Extended edge for Cypher Implementation
 * @param <K> the key type for the sources and target vertices
 * @param <V> the edge properties type
 * @param <E> the edge label type
 */

public class EdgeExtended<K, V extends HashMap<String, String>, E> extends Tuple4<K, K, V, E>{
	
	private static final long serialVersionUID = 1L;
	
	public EdgeExtended(){}

	public EdgeExtended(K srcId, K trgId, V props, E label) {
		this.f0 = srcId;
		this.f1 = trgId;
		this.f2 = props;
		this.f3 = label;
	}
	
	public void setSourceId(K srcId) {
		this.f0 = srcId;
	}

	public K getSourceId() {
		return this.f0;
	}

	public void setTargetId(K targetId) {
		this.f1 = targetId;
	}

	public K getTargetId() {
		return f1;
	}

	public void setProps(V props) {
		this.f2 = props;
	}

	public V getProps() {
		return f2;
	}
	
	public void setLabel(E label) {
		this.f3 = label;
	}

	public E getLabel() {
		return f3;
	}
	
	public boolean containsLabel(E labelInput) {
		if(labelInput.equals(f3))
			return true;
		else return false;
	}
	
	public boolean containsProps(V propsInput) {
		return true;
	}
}
