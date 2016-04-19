package operators.datastructures;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Extended vertex for Cypher Implementation
 * @param <K> the key type for the vertex ID
 * @param <L> the vertex label type
 * @param <P> the vertex properties type
 */
public class VertexExtended<K, L, P> extends Tuple3<K, L, P>{
	
	private static final long serialVersionUID = 1L;
	
	public VertexExtended(){};
	
	public VertexExtended(K vertexId, L labels, P props) {
		this.f0 = vertexId;
		this.f1 = labels;
		this.f2 = props;
	}
	
	public void setVertexId(K vertexId) {
		this.f0 = vertexId;
	}

	public K getVertexId() {
		return this.f0;
	}

	public void setLabels(L label) {
		this.f1 = label;
	}

	public L getLabels() {
		return f1;
	}
	
	public void setProps(P props) {
		this.f2 = props;
	}

	public P getProps() {
		return f2;
	}
	
		
	/*Check whether the input label matches the label of a indicated vertex*/
/*	public boolean containsLabel(E labelInput) {
		if(labelInput.equals(f2))
			return true;
		else return false;
	}*/
	
	/*Check whether the input props matches the label of a indicated vertex*/
/*	public boolean containsProps(V propsInput) {
		//if(propsInput.)	
		return true;
	}*/
}
