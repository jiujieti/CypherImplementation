package operators.datastructures;

import org.apache.flink.api.java.tuple.Tuple5;
/**
 * Extended edge for Cypher Implementation
 * @param <E> the key type for the edges
 * @param <K> the key type for the sources and target vertices
 * @param <L> the edge label type
 * @param <P> the edge properties type
 *  */

public class EdgeExtended<E, K, L, P> extends Tuple5<E, K, K, L, P>{
	
	private static final long serialVersionUID = 1L;
	
	public EdgeExtended(){}

	public EdgeExtended(E edgeId, K srcId, K trgId, L label, P props) {
		this.f0 = edgeId;
		this.f1 = srcId;
		this.f2 = trgId;
		this.f3 = label;
		this.f4 = props;
	}
	
	public void setEdgeId(E edgeId) {
		this.f0 = edgeId;
	}

	public E getEdgeId() {
		return this.f0;
	}
	
	public void setSourceId(K srcId) {
		this.f1 = srcId;
	}

	public K getSourceId() {
		return this.f1;
	}

	public void setTargetId(K targetId) {
		this.f2 = targetId;
	}

	public K getTargetId() {
		return f2;
	}

	public void setLabel(L label) {
		this.f3 = label;
	}

	public L getLabel() {
		return f3;
	}

	public void setProps(P props) {
		this.f4 = props;
	}	
	
	public P getProps() {
		return f4;
	}
		
}
