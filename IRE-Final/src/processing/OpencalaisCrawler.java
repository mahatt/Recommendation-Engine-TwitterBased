package processing;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.elmergarduno.jcalais.CalaisClient;
import net.elmergarduno.jcalais.CalaisObject;
import net.elmergarduno.jcalais.CalaisResponse;
import net.elmergarduno.jcalais.rest.CalaisRestClient;

import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;

import utility.DBUtility;

public class OpencalaisCrawler
{
	
	public static String API_KEY = "y7v4n7epq2pbx7knwknftjx7";
	//public static String API_KEY = "edg5u73fsfgh4x7erjjrvbn9"; not working 
	

	public static void getOpenCalaisSemanticsForNewsAndStoreInDB(String query)
	{
		Set<String> storedEntities = new HashSet();
		ResultSet news = DBUtility.executeQuery("Select distinct uri from semanticsEntityAttributes");
		try
		{
			while (news.next()) {
				storedEntities.add(news.getString("uri"));
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		CalaisClient client = null;
		news = DBUtility.executeQuery(query);
		try
		{
			String content = null;
			Integer newsId = null;
			Timestamp publish_date = null;
			String uri = null;
			Map<String, Object> entityAttributes = null;

			client = new CalaisRestClient(API_KEY);
			CalaisResponse response = null;

			BatchSqlUpdate su_JSON = new BatchSqlUpdate(
					DBUtility.ds, 
					"INSERT IGNORE INTO semanticsJSON (newsId, entities, topics, relations)  values (?,?,?,?)");

			su_JSON.declareParameter(new SqlParameter("newsId", -5));
			su_JSON.declareParameter(new SqlParameter("entities", 12));
			su_JSON.declareParameter(new SqlParameter("topics", 12));
			su_JSON.declareParameter(new SqlParameter("relations", 12));

			BatchSqlUpdate su_NewsEntity = new BatchSqlUpdate(
					DBUtility.ds, 
					"INSERT IGNORE INTO semanticsNewsEntity (newsId, type, typeURI, name, uri, relevance, publish_date)  values (?,?,?,?,?,?,?)");

			su_NewsEntity.declareParameter(new SqlParameter("newsId", -5));
			su_NewsEntity.declareParameter(new SqlParameter("type", 12));
			su_NewsEntity.declareParameter(new SqlParameter("typeURI", 12));
			su_NewsEntity.declareParameter(new SqlParameter("name", 12));
			su_NewsEntity.declareParameter(new SqlParameter("uri", 12));
			su_NewsEntity.declareParameter(new SqlParameter("relevance", 8));
			su_NewsEntity.declareParameter(new SqlParameter("publish_date", 93));

			BatchSqlUpdate su_NewsTopic = new BatchSqlUpdate(
					DBUtility.ds, 
					"INSERT IGNORE INTO semanticsNewsTopic (newsId, topic, uri, relevance, publish_date)  values (?,?,?,?,?)");

			su_NewsTopic.declareParameter(new SqlParameter("newsId", -5));
			su_NewsTopic.declareParameter(new SqlParameter("topic", 12));
			su_NewsTopic.declareParameter(new SqlParameter("uri", 12));
			su_NewsTopic.declareParameter(new SqlParameter("relevance", 8));
			su_NewsEntity.declareParameter(new SqlParameter("publish_date", 93));


			BatchSqlUpdate su_EntityAttributes = new BatchSqlUpdate(
					DBUtility.ds, 
					"INSERT IGNORE INTO semanticsEntityAttributes (uri, attribute, value)  values (?,?,?)");

			su_EntityAttributes.declareParameter(new SqlParameter("uri", 12));
			su_EntityAttributes.declareParameter(new SqlParameter("attribute", 12));
			su_EntityAttributes.declareParameter(new SqlParameter("value", 12));



			int count = 0;int storeAfter = 50;
			while (news.next()) {
				try
				{
					count++;
					content = news.getString("newscontent");
					newsId = Integer.valueOf(news.getInt("id"));
					publish_date = news.getTimestamp("publish_date");

					System.out.println("Processing News " + newsId);


					response = client.analyze(content);


					Object[] json = {
							newsId, 
							response.getEntities() != null ? response.getEntities().toString() : null, 
									response.getTopics() != null ? response.getTopics().toString() : null, 
											response.getRelations() != null ? response.getRelations().toString() : null };

					su_JSON.update(json);
					try
					{
						if (response.getEntities() != null) {
							for (CalaisObject entity : response.getEntities())
							{
								uri = entity.getField("_uri");
								if (entity.getList("resolutions") != null)
								{
									entityAttributes = (Map)entity.getList("resolutions").iterator().next();
									if ((entityAttributes != null) && (entityAttributes.containsKey("id"))) {
										uri = entityAttributes.get("id").toString();
									}
									try
									{
										if (!storedEntities.contains(uri))
										{
											for (String attribute : entityAttributes.keySet())
											{
												Object[] attributes = {
														uri, 
														attribute, 
														entityAttributes.get(attribute) };

												su_EntityAttributes.update(attributes);
											}
											storedEntities.add(uri);
										}
									}
									catch (Exception e)
									{
										System.err.println("Problems while stroing entity attributes: " + e.getMessage());
									}
								}
								Object[] newsEntity = {
										newsId, 
										entity.getField("_type"), 
										entity.getField("_typeReference"), 
										entity.getField("name"), 
										uri, 
										entity.getField("relevance") != null ? entity.getField("relevance") : Double.valueOf(0.0D), 
												publish_date };

								su_NewsEntity.update(newsEntity);
							}
						}
					}
					catch (Exception e)
					{
						System.err.println("Problems while stroing news entity assignments: " + e.getMessage());
					}
					try
					{
						if (response.getTopics() != null) {
							for (CalaisObject topic : response.getTopics())
							{
								Object[] newsTopic = {
										newsId, 
										topic.getField("categoryName"), 
										topic.getField("category"), 
										topic.getField("score") != null ? topic.getField("score") : Double.valueOf(0.0D), 
												publish_date };

								su_NewsTopic.update(newsTopic);
							}
						}
					}
					catch (Exception e)
					{
						System.err.println("Problems while stroing news topic assignments: " + e.getMessage());
					}
					if (count % storeAfter == 0)
					{
						su_JSON.flush();
						su_EntityAttributes.flush();
						su_NewsEntity.flush();
						su_NewsTopic.flush();
						System.out.println("\n*********************\n* Processed " + count + " news articles.\n**********************\n");
					}
				}
				catch (Exception e)
				{
					e.printStackTrace();
					if ((e.getMessage() != null) && (e.getMessage().contains("returned a response status of 403")) && (e.getMessage().toLowerCase().contains("response")))
					{
						switchAPIKey();
						client = new CalaisRestClient(API_KEY);
						System.out.println("####\n#### New API key: " + API_KEY);
					}
				}
			}
			su_JSON.flush();
			su_EntityAttributes.flush();
			su_NewsEntity.flush();
			su_NewsTopic.flush();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void getOpenCalaisSemanticsForTweetsAndStoreInDB(Integer userId, String query)
	{
		Set<String> storedEntities = new HashSet();
		System.out.println("Gathering Uris already stored...");
		ResultSet tweets = null;
		tweets = DBUtility.executeQuery("Select distinct uri from semanticsEntityAttributes");
		try
		{
			int count = 0;
			while (tweets.next())
			{
				storedEntities.add(tweets.getString("uri"));
				count++;
				if (count % 10000 == 0) {
					System.out.println(count + " Uris gathered.");
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		System.out.println("Querying for tweets...");
		System.out.println(query);
		tweets = DBUtility.executeQuery(query);
		try
		{
			String content = null;
			Long tweetId = null;
			String uri = null;
			Timestamp creationTime = null;
			Map<String, Object> entityAttributes = null;

			CalaisClient client = new CalaisRestClient(API_KEY);
			CalaisResponse response = null;

			BatchSqlUpdate su_JSON = new BatchSqlUpdate(
					DBUtility.ds, 
					"INSERT IGNORE INTO semanticsTweetsJSON (userId, tweetId, entities, topics, relations)  values (?,?,?,?,?)");

			su_JSON.declareParameter(new SqlParameter("userId", 4));
			su_JSON.declareParameter(new SqlParameter("tweetId", -5));
			su_JSON.declareParameter(new SqlParameter("entities", 12));
			su_JSON.declareParameter(new SqlParameter("topics", 12));
			su_JSON.declareParameter(new SqlParameter("relations", 12));

			BatchSqlUpdate su_NewsEntity = new BatchSqlUpdate(
					DBUtility.ds, 
					"INSERT IGNORE INTO semanticsTweetsEntity (userId, tweetId, type, typeURI, name, uri, relevance, creationTime)  values (?,?,?,?,?,?,?,?)");

			su_NewsEntity.declareParameter(new SqlParameter("userId", 4));
			su_NewsEntity.declareParameter(new SqlParameter("tweetId", -5));
			su_NewsEntity.declareParameter(new SqlParameter("type", 12));
			su_NewsEntity.declareParameter(new SqlParameter("typeURI", 12));
			su_NewsEntity.declareParameter(new SqlParameter("name", 12));
			su_NewsEntity.declareParameter(new SqlParameter("uri", 12));
			su_NewsEntity.declareParameter(new SqlParameter("relevance", 8));
			su_NewsEntity.declareParameter(new SqlParameter("creationTime", 93));

			BatchSqlUpdate su_NewsTopic = new BatchSqlUpdate(
					DBUtility.ds, 
					"INSERT IGNORE INTO semanticsTweetsTopic (userId, tweetId, topic, uri, relevance, creationTime)  values (?,?,?,?,?,?)");

			su_NewsTopic.declareParameter(new SqlParameter("userId", 4));
			su_NewsTopic.declareParameter(new SqlParameter("tweetId", -5));
			su_NewsTopic.declareParameter(new SqlParameter("topic", 12));
			su_NewsTopic.declareParameter(new SqlParameter("uri", 12));
			su_NewsTopic.declareParameter(new SqlParameter("relevance", 8));
			su_NewsTopic.declareParameter(new SqlParameter("creationTime", 93));


			BatchSqlUpdate su_EntityAttributes = new BatchSqlUpdate(
					DBUtility.ds, 
					"INSERT IGNORE INTO semanticsEntityAttributes (uri, attribute, value)  values (?,?,?)");

			su_EntityAttributes.declareParameter(new SqlParameter("uri", 12));
			su_EntityAttributes.declareParameter(new SqlParameter("attribute", 12));
			su_EntityAttributes.declareParameter(new SqlParameter("value", 12));



			int count = 0;int storeAfter = 50;
			while (tweets.next()) {
				try
				{
					count++;
					content = tweets.getString("content");
					tweetId = Long.valueOf(tweets.getLong("id"));
					creationTime = tweets.getTimestamp("creationTime");

					System.out.println("Processing Tweet " + tweetId);


					response = client.analyze(content);


					Object[] json = {
							userId, 
							tweetId, 
							response.getEntities() != null ? response.getEntities().toString() : null, 
									response.getTopics() != null ? response.getTopics().toString() : null, 
											response.getRelations() != null ? response.getRelations().toString() : null };

					su_JSON.update(json);
					try
					{
						if (response.getEntities() != null) {
							for (CalaisObject entity : response.getEntities())
							{
								uri = entity.getField("_uri");
								if (entity.getList("resolutions") != null)
								{
									entityAttributes = (Map)entity.getList("resolutions").iterator().next();
									if ((entityAttributes != null) && (entityAttributes.containsKey("id"))) {
										uri = entityAttributes.get("id").toString();
									}
									try
									{
										if (!storedEntities.contains(uri))
										{
											for (String attribute : entityAttributes.keySet())
											{
												Object[] attributes = {
														uri, 
														attribute, 
														entityAttributes.get(attribute) };

												su_EntityAttributes.update(attributes);
											}
											storedEntities.add(uri);
										}
									}
									catch (Exception e)
									{
										System.err.println("Problems while storing entity attributes: " + e.getMessage());
									}
								}
								Object[] newsEntity = {
										userId, 
										tweetId, 
										entity.getField("_type"), 
										entity.getField("_typeReference"), 
										entity.getField("name"), 
										uri, 
										entity.getField("relevance") != null ? entity.getField("relevance") : Double.valueOf(0.0D), 
												creationTime };

								su_NewsEntity.update(newsEntity);
							}
						}
					}
					catch (Exception e)
					{
						System.err.println("Problems while storing tweet entity assignments: " + e.getMessage());
					}
					try
					{
						if (response.getTopics() != null) {
							for (CalaisObject topic : response.getTopics())
							{
								Object[] newsTopic = {
										userId, 
										tweetId, 
										topic.getField("categoryName"), 
										topic.getField("category"), 
										topic.getField("score") != null ? topic.getField("score") : Double.valueOf(0.0D), 
												creationTime };

								su_NewsTopic.update(newsTopic);
							}
						}
					}
					catch (Exception e)
					{
						System.err.println("Problems while stroing tweet topic assignments: " + e.getMessage());
					}
					if (count % storeAfter == 0)
					{
						su_JSON.flush();
						su_EntityAttributes.flush();
						su_NewsEntity.flush();
						su_NewsTopic.flush();
						System.out.println("\n*********************\n* Processed " + count + " tweets.\n**********************\n");
					}
				}
				catch (Exception e)
				{
					e.printStackTrace();
					if ((e.getMessage() != null) && (e.getMessage().contains("returned a response status of 403")) && (e.getMessage().toLowerCase().contains("response")))
					{
						switchAPIKey();
						client = new CalaisRestClient(API_KEY);
						System.out.println("####\n#### New API key: " + API_KEY);
					}
				}
			}
			su_JSON.flush();
			su_EntityAttributes.flush();
			su_NewsEntity.flush();
			su_NewsTopic.flush();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	/*
	private static void testing()
	{
		CalaisClient client = new CalaisRestClient(API_KEY);
		CalaisResponse response = null;
		ResultSet news = DBUtility.executeQuery("Select title, newscontent from news where id = 2150");
		try
		{
			String title = null;
			String content = null;
			for (; news.next(); news.hasNext())
			{
				title = news.getString("title");
				System.out.println("Title: " + title);

				content = news.getString("newscontent");
				response = client.analyze(content);
				System.out.println("####  Entities: ");

				for (CalaisObject entity : response.getEntities())
				{
					System.out.println(entity);
					System.out.println(entity.getField("_type") + ":" + 
							entity.getField("name") + " " + 
							entity.getField("nationality") + " " + 
							entity.getField("relevance") + " " + 
							entity.getField("_uri"));
					if (entity.getList("resolutions") != null)
					{
						Map<String, Object> details = (Map)entity.getList("resolutions").iterator().next();
						System.out.println("ID: " + details.get("id"));
						for (String attribute : details.keySet()) {
							System.out.println("  - " + attribute + ": " + details.get(attribute));
						}
					}
					System.out.println("");
				}
				System.out.println("####  Topics: ");

				System.out.println(" - - - - ");
				??? = response.getTopics().iterator();
				continue;
				CalaisObject topic = (CalaisObject)???.next();
				System.out.println(topic.getField("categoryName") + "  " + topic.getField("_uri") + "  " + topic.getField("score") + "  " + topic);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	 */	public static void processTweets(String[] args)
	 {
		 String userIdFile = null;
		 if ((args != null) && (args.length == 2))
		 {
			 API_KEY = args[1];
			 userIdFile = args[0];
		 }
		 else
		 {
			 System.out.println("Specify the file which contains the userIds and the OpenCalais API key:\njava -jar um-opencalais-tweets-enrichment.jar /path/to/userIdFile apikey");
		 }
		 Set<Integer> userIds = DataPreAnalysis.getUserIds(20, 10, true, userIdFile);
		 List<Integer> userIdsOrdered = new ArrayList();
		 userIdsOrdered.addAll(userIds);
		 Collections.sort(userIdsOrdered);
		 boolean userIdDone = false;


		 int estimate = 100;
		 double count = 0.0D;
		 double size = new Integer(userIdsOrdered.size()).doubleValue();
		 for (Integer userId : userIdsOrdered)
		 {
			 try
			 {
				 ResultSet results = DBUtility.executeQuery(
						 "Select count(t.id) from  tweets t WHERE t.userId = " + 
								 userId + " " + 
								 "AND t.id in (Select distinct tweetId FROM semanticsTweetsJSON s WHERE s.userId = " + userId + " ) ");
				 if ((results != null) && (results.next()))
				 {
					 if (results.getInt(1) > estimate) {
						 userIdDone = true;
					 } else {
						 userIdDone = false;
					 }
				 }
				 else {
					 userIdDone = false;
				 }
			 }
			 catch (Exception localException) {}
			 if (!userIdDone) {
				 getOpenCalaisSemanticsForTweetsAndStoreInDB(userId, 
						 "SELECT distinct t.id, t.content, creationTime FROM tweets t WHERE t.userId = " + 
								 userId + " " + 
								 "AND t.id NOT in (Select distinct tweetId FROM semanticsTweetsJSON s WHERE s.userId = " + userId + " )  ");
			 }
			 count += 1.0D;
			 System.out.println(count / size + " of the users are processed.");
		 }
	 }

	 public static void main(String[] args)
	 {
		 processTweets(args);
		 if ((args != null) && (args.length == 2)) 
		 {
			 API_KEY = args[1];
		 }
	 }
}


