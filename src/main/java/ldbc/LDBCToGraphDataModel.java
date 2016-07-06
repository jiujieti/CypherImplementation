package ldbc;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;

import ldbc.group.*;
import ldbc.join.*;
import ldbc.map.*;
import operators.datastructures.*;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.core.fs.FileSystem;


public class LDBCToGraphDataModel {
	private String dir;
	private ExecutionEnvironment env;
	public LDBCToGraphDataModel(String dir, ExecutionEnvironment env) {
		
		this.dir = dir;
		this.env = env;
	}
	
	public void getGraph() throws Exception { 
		//String dir = "C:/Datasets/ldbc_dataset_small/";
		DataSet<Tuple6<Long, String, String, String, String, String>> comments = env.readCsvFile(dir + "comment_0_0.csv")
				.ignoreFirstLine()
				.fieldDelimiter("|")
				.types(Long.class, String.class, String.class, String.class, String.class, String.class);
		final String[] commentItems = {"comment", "id", "creationDate", "locationIP", "browserUsed", "content", "length"};
				
		
		DataSet<Tuple3<Long, String, String>> forums = env.readCsvFile(dir + "forum_0_0.csv")
				.ignoreFirstLine()
				.fieldDelimiter("|")
				.types(Long.class, String.class, String.class);
		final String[] forumItems = {"forum", "id", "title", "creationDate"};
		
		DataSet<Tuple3<Long, String, String>> tags = env.readCsvFile(dir + "tag_0_0.csv")
				.ignoreFirstLine()
				.fieldDelimiter("|")
				.types(Long.class, String.class, String.class);
		final String[] tagItems = {"tag", "id", "name", "url"};

		
		DataSet<Tuple3<Long, String, String>> tagclasses = env.readCsvFile(dir + "tagclass_0_0.csv")
				.ignoreFirstLine()
				.fieldDelimiter("|")
				.types(Long.class, String.class, String.class);
		final String[] tagclassItems = {"tagclass", "id", "name", "url"};
		
		DataSet<Tuple8<Long, String, String, String, String, String, String, String>> posts = env.readCsvFile(dir + "post_0_0.csv")
				.ignoreFirstLine()
				.fieldDelimiter("|")
				.types(Long.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class);
		final String[] postItems = {"post", "id", "imageFile", "creationDate", "locationIP", "browserUsed", "language", "content", "length"};
		
		DataSet<Tuple4<Long, String, String, String>> places = env.readCsvFile(dir + "place_0_0.csv")
				.ignoreFirstLine()
				.fieldDelimiter("|")
				.types(Long.class, String.class, String.class, String.class);
		final String[] placeItems = {"place", "id", "name", "url", "type"};
		
		DataSet<Tuple4<Long, String, String, String>> organisation = env.readCsvFile(dir + "organisation_0_0.csv")
				.ignoreFirstLine()
				.fieldDelimiter("|")
				.types(Long.class, String.class, String.class, String.class);
		final String[] organisationItems = {"organisation", "id", "type", "name", "url"};
		
		DataSet<Tuple8<Long, String, String, String, String, String, String, String>> person = env.readCsvFile(dir + "person_0_0.csv")
				.ignoreFirstLine()
				.fieldDelimiter("|")
				.types(Long.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class);
		
		DataSet<Tuple9<Long, String, String, String, String, String, String, String, String>> personWithEmail = env.readCsvFile(dir + "person_email_emailaddress_0_0.csv")
				.ignoreFirstLine()
				.fieldDelimiter("|")
				.types(Long.class, String.class)
				.coGroup(person)
				.where(0)
				.equalTo(0)
				.with(new PersonGroupEmail());

		DataSet<Tuple10<Long, String, String, String, String, String, String, String, String, String>> personWithEmailAndLanguage = env.readCsvFile(dir + "person_speaks_language_0_0.csv")
				.ignoreFirstLine()
				.fieldDelimiter("|")
				.types(Long.class, String.class)
				.coGroup(personWithEmail)
				.where(0)
				.equalTo(0)
				.with(new PersonGroupLanguage());

		
		
		final String[] personItems = {"person", "id", "firstName", "lastName", "gender", "birthday", "creationDate", "locationIP", "browserUsed", "email", "language"};
		
		DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> commentVertices = comments
				.map(new CommentMap(commentItems)); 
		
		DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> commentMaxId = commentVertices.max(0);
		
		DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> forumVertices = commentMaxId
				.cross(forums)
				.with(new ForumMap(forumItems));
		
		DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> forumMaxId = forumVertices.max(0);
		
		DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> tagVertices = forumMaxId
				.cross(tags)
				.with(new TagMap(tagItems));
		
		DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> tagMaxId = tagVertices.max(0);
		
		DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> tagclassVertices = tagMaxId
				.cross(tagclasses)
				.with(new TagClassMap(tagclassItems));
		
		DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> tagclassMaxId = tagclassVertices.max(0);
		
		DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> postVertices = tagclassMaxId
				.cross(posts)
				.with(new PostMap(postItems));

		DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> postMaxId = postVertices.max(0);
		
		DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> placeVertices = postMaxId
				.cross(places)
				.with(new PlaceMap(placeItems));

		DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> placeMaxId = placeVertices.max(0);
		
		DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> organisationVertices = placeMaxId
				.cross(organisation)
				.with(new OrganisationMap(organisationItems));

		DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> organisationMaxId = organisationVertices.max(0);

		DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> personVertices = organisationMaxId
				.cross(personWithEmailAndLanguage)
				.with(new PersonMap(personItems));
		
		DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> personMaxId = personVertices.max(0);

		DataSet<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> vertices = commentVertices
				.union(forumVertices)
				.union(tagVertices)
				.union(tagclassVertices)
				.union(placeVertices)
				.union(organisationVertices)
				.union(postVertices)
				.union(personVertices)
				.map(new DeleteOriginalId());
		
		EdgeIdReplacerLeft getSourceIds = new EdgeIdReplacerLeft();
		EdgeIdReplacerRight getTargetIds = new EdgeIdReplacerRight();
		
		//change to uniqueId
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasCreatorFromComment = env.readCsvFile(dir + "comment_hasCreator_person_0_0.csv")
				.ignoreFirstLine()
				.fieldDelimiter("|")
				.types(Long.class, Long.class)
				.join(commentVertices)
				.where(0)
				.equalTo(3)
				.with(getSourceIds)
				.join(personVertices)
				.where(1)
				.equalTo(3)
				.with(getTargetIds)
				.cross(personMaxId)
				.with(new HasCreatorMap("hasCreator"));
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasCreatorFromCommentMaxId = hasCreatorFromComment.max(0);
		
		
		//Two elements, input: csv, joinLeftSet, joinRightSet, lastMax, label
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasTagFromComment = 
				getEdges(dir + "comment_hasTag_tag_0_0.csv", 
						commentVertices,
						tagVertices,
						hasCreatorFromCommentMaxId,
						"hasTag");
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasTagFromCommentMaxId = hasTagFromComment.max(0);
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> isLocatedInFromComment = 
				getEdges(dir + "comment_isLocatedIn_place_0_0.csv", 
						commentVertices,
						placeVertices,
						hasTagFromCommentMaxId,
						"isLocatedIn");
				
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> isLocatedInFromCommentMaxId = isLocatedInFromComment.max(0);
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> replyOfComment = 
				getEdges(dir + "comment_replyOf_comment_0_0.csv", 
						commentVertices,
						commentVertices,
						isLocatedInFromCommentMaxId,
						"replyOf");

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> replyOfCommentMaxId = replyOfComment.max(0);

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> replyOfPost = 
				getEdges(dir + "comment_replyOf_post_0_0.csv", 
						commentVertices,
						postVertices,
						replyOfCommentMaxId,
						"replyOf");

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> replyOfPostMaxId = replyOfPost.max(0);
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> containerOf = 
				getEdges(dir + "forum_containerOf_post_0_0.csv", 
						forumVertices,
						postVertices,
						replyOfPostMaxId,
						"containerOf");

		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> containerOfMaxId = containerOf.max(0);

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasMember = getEdgesWithThreeElements(
				dir + "forum_hasMember_person_0_0.csv",
				forumVertices,
				personVertices,
				containerOfMaxId,
				"hasMember",
				"joinDate");
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasMemberMaxId = hasMember.max(0);

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasModerator = 
				getEdges(dir + "forum_hasModerator_person_0_0.csv", 
						forumVertices,
						personVertices,
						hasMemberMaxId,
						"hasModerator");

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasModeratorMaxId = hasModerator.max(0);

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasTagFromForum = 
				getEdges(dir + "forum_hasTag_tag_0_0.csv", 
						forumVertices,
						tagVertices,
						hasModeratorMaxId ,
						"hasTag");

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasTagFromForumMaxId = hasTagFromForum.max(0);
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> isLocatedInFromOrganisation = 
				getEdges(dir + "organisation_isLocatedIn_place_0_0.csv", 
						organisationVertices,
						placeVertices,
						hasTagFromForumMaxId,
						"isLocatedIn");
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> isLocatedFromOrganisationMaxId = isLocatedInFromOrganisation.max(0);
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasInterest = 
				getEdges(dir + "person_hasInterest_tag_0_0.csv", 
						personVertices,
						tagVertices,
						isLocatedFromOrganisationMaxId,
						"hasInterest");
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasInterestMaxId = hasInterest.max(0);
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> isLocatedInFromPerson = 
				getEdges(dir + "person_isLocatedIn_place_0_0.csv", 
						personVertices,
						placeVertices,
						hasInterestMaxId,
						"isLocatedIn");
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> isLocatedInFromPersonMaxId = isLocatedInFromPerson.max(0);
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> knows = getEdgesWithThreeElements(
				dir + "person_knows_person_0_0.csv",
				personVertices,
				personVertices,
				isLocatedInFromPersonMaxId,
				"knows",
				"creationDate");

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> knowsMaxId = knows.max(0);
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> likesComment = getEdgesWithThreeElements(
				dir + "person_likes_comment_0_0.csv",
				personVertices,
				commentVertices,
				knowsMaxId,
				"likes",
				"creationDate");

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> likesCommentMaxId = likesComment.max(0);

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> likesPost = getEdgesWithThreeElements(
				dir + "person_likes_post_0_0.csv",
				personVertices,
				postVertices,
				likesCommentMaxId,
				"likes",
				"creationDate");
		
//		PrintWriter writer = new PrintWriter(dir + "out.txt", "UTF-8");
	//	writer.println("likesPost.count() == " + likesPost.count());
		//writer.close();

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> likesPostMaxId = likesPost.max(0);

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> studyAt = getEdgesWithThreeElements(
				dir + "person_studyAt_organisation_0_0.csv",
				personVertices,
				organisationVertices,
				likesPostMaxId,
				"studyAt",
				"classYear");

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> studyAtMaxId = studyAt.max(0);

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> workAt = getEdgesWithThreeElements(
				dir + "person_workAt_organisation_0_0.csv",
				personVertices,
				organisationVertices,
				studyAtMaxId,
				"workAt",
				"workFrom");

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> workAtMaxId = workAt.max(0);

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> isPartOf = 
				getEdges(dir + "place_isPartOf_place_0_0.csv", 
						placeVertices,
						placeVertices,
						workAtMaxId,
						"isPartOf");

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> isPartOfMaxId = isPartOf.max(0);
		

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasCreatorFromPost = 
				getEdges(dir + "post_hasCreator_person_0_0.csv", 
						postVertices,
						personVertices,
						isPartOfMaxId,
						"hasCreator");

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasCreatorFromPostMaxId = hasCreatorFromPost.max(0);

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasTagFromPost = 
				getEdges(dir + "post_hasTag_tag_0_0.csv", 
						postVertices,
						tagVertices,
						hasCreatorFromPostMaxId,
						"hasCreator");

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasTagFromPostMaxId = hasTagFromPost.max(0);
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> isLocatedInFromPost = 
				getEdges(dir + "post_isLocatedIn_place_0_0.csv", 
						postVertices,
						placeVertices,
						hasTagFromPostMaxId,
						"isLocatedIn");

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> isLocatedInFromPostMaxId = isLocatedInFromPost.max(0);

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasType = 
				getEdges(dir + "tag_hasType_tagclass_0_0.csv", 
						tagVertices,
						tagclassVertices,
						isLocatedInFromPostMaxId,
						"hasType");

		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasTypeMaxId = hasType.max(0);
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> isSubclassOf = 
				getEdges(dir + "tagclass_isSubclassOf_tagclass_0_0.csv", 
						tagclassVertices,
						tagclassVertices,
						hasTypeMaxId,
						"isSubclassOf");		
		
		DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> edges = hasCreatorFromComment
				.union(hasTagFromComment)
				.union(isLocatedInFromComment)
				.union(replyOfComment)
				.union(replyOfPost)
				.union(containerOf)
				.union(hasMember)
				.union(hasModerator)
				.union(hasTagFromForum)
				.union(isLocatedInFromOrganisation)
				.union(hasInterest)
				.union(isLocatedInFromPerson)
				.union(knows)
				.union(likesPost)
				.union(likesComment)
				.union(studyAt)
				.union(workAt)
				.union(isPartOf)
				.union(hasCreatorFromPost)
				.union(hasTagFromPost)
				.union(isLocatedInFromPost)
				.union(hasType)
				.union(isSubclassOf);
		
//		GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> dataGraph = GraphExtended.fromDataSet(vertices, edges, env);
		vertices.writeAsCsv(dir + "vertices.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);
		edges.writeAsCsv(dir + "edges.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);
		
		env.setParallelism(1);
		env.execute();
		//return personWithEmailAndLanguage;
	}
	
	private DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> getEdges(
			String path,
			DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> verticesLeft, 
			DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> verticesRight,
			DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> lastMaxId,
			String label) {
		EdgeIdReplacerLeft getSourceIds = new EdgeIdReplacerLeft();
		EdgeIdReplacerRight getTargetIds = new EdgeIdReplacerRight();
		return env.readCsvFile(path)
				.ignoreFirstLine()
				.fieldDelimiter("|")
				.types(Long.class, Long.class)
				.join(verticesLeft)
				.where(0)
				.equalTo(3)
				.with(getSourceIds)
				.join(verticesRight)
				.where(1)
				.equalTo(3)
				.with(getTargetIds)
				.cross(lastMaxId)
				.with(new EdgeWithTwoElementsMap(label));
		
	}
	
	private DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> getEdgesWithThreeElements(
			String path,
			DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> verticesLeft, 
			DataSet<Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>> verticesRight,
			DataSet<EdgeExtended<Long, Long, String, HashMap<String, String>>> lastMaxId,
			String label,
			String key) {
		EdgeIdReplacerLeftThreeElements getSourceIds = new EdgeIdReplacerLeftThreeElements();
		EdgeIdReplacerRightThreeElements getTargetIds = new EdgeIdReplacerRightThreeElements();
		return env.readCsvFile(path)
				.ignoreFirstLine()
				.fieldDelimiter("|")
				.types(Long.class, Long.class, String.class)
				.join(verticesLeft)
				.where(0)
				.equalTo(3)
				.with(getSourceIds)
				.join(verticesRight)
				.where(1)
				.equalTo(3)
				.with(getTargetIds)
				.cross(lastMaxId)
				.with(new EdgeWithThreeElementsMap(label, key));
		
	}

}
