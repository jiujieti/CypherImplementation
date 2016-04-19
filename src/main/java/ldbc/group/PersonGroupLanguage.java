package ldbc.group;

import java.util.Iterator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class PersonGroupLanguage implements CoGroupFunction<Tuple2<Long, String>, Tuple9<Long, String, String, String, String, String, String, String, String>,
	Tuple10<Long, String, String, String, String, String, String, String, String, String>> {

	@Override
	public void coGroup(
			Iterable<Tuple2<Long, String>> languages,
			Iterable<Tuple9<Long, String, String, String, String, String, String, String, String>> person,
			Collector<Tuple10<Long, String, String, String, String, String, String, String, String, String>> personWithEmailAndLanguage)
			throws Exception{

		String language = "";
		Iterator<Tuple2<Long, String>> languageItr = languages.iterator();
		Iterator<Tuple9<Long, String, String, String, String, String, String, String, String>> personItr = person.iterator();
		while(languageItr.hasNext())
			language += languageItr.next().f1 + ", ";
		

		Tuple9<Long, String, String, String, String, String, String, String, String> prs = personItr.next();

		if(language.length() > 2)
			personWithEmailAndLanguage.collect(new Tuple10<Long, String, String, String, String, String, String, String, String, String>(prs.f0, prs.f1, prs.f2, prs.f3, prs.f4, prs.f5, prs.f6, prs.f7, prs.f8, language.substring(0, language.length() - 2)));
		else
			personWithEmailAndLanguage.collect(new Tuple10<Long, String, String, String, String, String, String, String, String, String>(prs.f0, prs.f1, prs.f2, prs.f3, prs.f4, prs.f5, prs.f6, prs.f7, prs.f8, language));
		
	}
}
