package ldbc.group;

import java.util.Iterator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class PersonGroupEmail implements CoGroupFunction<Tuple2<Long, String>, Tuple8<Long, String, String, String, String, String, String, String>,
		Tuple9<Long, String, String, String, String, String, String, String, String>> {

	@Override
	public void coGroup(
			Iterable<Tuple2<Long, String>> emails,
			Iterable<Tuple8<Long, String, String, String, String, String, String, String>> person,
			Collector<Tuple9<Long, String, String, String, String, String, String, String, String>> personWithEmail)
			throws Exception {
		
		String email = "";
		Iterator<Tuple2<Long, String>> emailItr = emails.iterator();
		Iterator<Tuple8<Long, String, String, String, String, String, String, String>> personItr = person.iterator();
		while(emailItr.hasNext()) {
			email += emailItr.next().f1 + ", ";
		}
		
		//email += emailItr.next().f1;
		
		Tuple8<Long, String, String, String, String, String, String, String> prs = personItr.next();
		
		if(email.length() > 0)
			personWithEmail.collect(new Tuple9<Long, String, String, String, String, String, String, String, String>(prs.f0, prs.f1, prs.f2, prs.f3, prs.f4, prs.f5, prs.f6, prs.f7, email.substring(0, email.length() - 2)));
		else
			personWithEmail.collect(new Tuple9<Long, String, String, String, String, String, String, String, String>(prs.f0, prs.f1, prs.f2, prs.f3, prs.f4, prs.f5, prs.f6, prs.f7, email));	
	}

}
