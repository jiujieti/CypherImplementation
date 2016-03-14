package MyProject.DataStructures;

import java.util.ArrayList;
import java.io.Serializable;
import java.lang.Comparable;



public class VertexAndEdgeIds<K> implements Comparable, Serializable{
	
	private static final long serialVersionUID = 1L;
	
	private ArrayList<K> ids = new ArrayList<K>();
	
	VertexAndEdgeIds() {}
	
	VertexAndEdgeIds(ArrayList<K> idsOfEdgesAndVertices) {
		this.ids = idsOfEdgesAndVertices;
	}
	
}