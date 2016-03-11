package MyProject.DataStructures;

import org.apache.flink.api.java.tuple.Tuple3;
import java.util.HashMap;;
/**
 * Extended vertex for Cypher Implementation
 * @param <K> the key type for the vertex ID
 * @param <V> the vertex properties type
 * @param <E> the vertex label type
 */
public class VertexExtended<K, V extends HashMap<String, String>, E> extends Tuple3<K, V, E>{
	
	private static final long serialVersionUID = 1L;
	
	public VertexExtended(){};
	
	public VertexExtended(K vertexId, V props, E label) {
		this.f0 = vertexId;
		this.f1 = props;
		this.f2 = label;
	}
	
	public void setVertexId(K vertexId) {
		this.f0 = vertexId;
	}

	public K getVertexId() {
		return this.f0;
	}


	public void setProps(V props) {
		this.f1 = props;
	}

	public V getProps() {
		return f1;
	}
	
	public void setLabel(E label) {
		this.f2 = label;
	}

	public E getLabel() {
		return f2;
	}
	
	/*Check whether the input label matches the label of a indicated vertex*/
	public boolean containsLabel(E labelInput) {
		if(labelInput.equals(f2))
			return true;
		else return false;
	}
	
	/*Check whether the input props matches the label of a indicated vertex*/
	public boolean containsProps(V propsInput) {
		//if(propsInput.)	
		return true;
	}
}
