package ldbc.map;

import java.util.HashMap;
import java.util.HashSet;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;

@SuppressWarnings("serial")
public class CommentMap implements MapFunction<Tuple6<Long, String, String, String, String, String>,
		Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> {

	private String[] commentItems;
	private long newId = 0;
	
	public CommentMap(String[] commentItems) {
		this.commentItems = commentItems;
	}

	@Override
	public Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>
 		map(Tuple6<Long, String, String, String, String, String> comment)
 				throws Exception {

		Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> commentWithOriginId = new Tuple4<>();

		//set vertex id

		commentWithOriginId.f0 = newId;
		this.newId ++;

		//set vertex labels
		HashSet<String> labels = new HashSet<>();
		labels.add(commentItems[0]);
		commentWithOriginId.f1 = labels;

		//set vertex properties
		HashMap<String, String> properties = new HashMap<>();
		properties.put(commentItems[1], comment.f0.toString());
		properties.put(commentItems[2], comment.f1.toString());
		properties.put(commentItems[3], comment.f2.toString());
		properties.put(commentItems[4], comment.f3.toString());
		properties.put(commentItems[5], comment.f4.toString());
		properties.put(commentItems[6], comment.f5.toString());
		commentWithOriginId.f2 = properties;
		
		commentWithOriginId.f3 = comment.f0;
		return commentWithOriginId;
	}
}
