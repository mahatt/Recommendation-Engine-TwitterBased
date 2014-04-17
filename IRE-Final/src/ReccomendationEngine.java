import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;



import utility.DatabaseUtility;

import com.mysql.jdbc.ResultSet;


public class ReccomendationEngine 
{
	String entityprofilesim="D:\\Dataset\\EntityProfileSimilarity-Year\\";
	String entityprofile="D:\\Dataset\\EnityProfileVector-Year\\";

	/*ENTITY*/
	public TreeMap<Double,String> loadUserSimilarityEntityProfile(long userId)
	{
		TreeMap<Double,String> map = new TreeMap<Double,String>(Collections.reverseOrder());
		try
		{			
			BufferedReader breader = new BufferedReader(new FileReader(new File(entityprofilesim+userId+".txt")));
			String line=breader.readLine();
			while(line!=null)
			{
				map.put(Double.valueOf(line.split(" ")[1]), line.split(" ")[0]);
				line=breader.readLine();
			}	
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		return map;
	}

	public void loadEntityProfileOfUser(long userId)
	{
		TreeMap<String,Integer> list = new TreeMap<String,Integer>();
		try
		{
			BufferedReader breader = new BufferedReader(new FileReader(new File(entityprofilesim+userId+".txt")));
			String line=breader.readLine();
			while(line!=null)
			{
				//map.put(Double.valueOf(line.split(" ")[0]));
				line=breader.readLine();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public void compareEntitySet(long userId,long sim_user)
	{

	}
	public void loadEntityForSimilarUsers(long userId,Timestamp from,Timestamp to)
	{
		try
		{
			TreeMap<Double,String>map = loadUserSimilarityEntityProfile(userId);
			long sim_user;
			String list=null;
			int count=0;
			for(Map.Entry<Double,String>  entry : map.entrySet())
			{
				if(count>2) break;
				count++;
				sim_user = Long.valueOf(entry.getValue());				
				System.out.println("***** Similar User "+sim_user + " *****");				


				String query ="SELECT newsId,type,publish_date FROM twitter.semanticsnewsentity where publish_date >=\""+from
						+"\" AND publish_date <=\""+to+ "\""+ 
						"order by relevance desc limit 0,3";
				//System.out.println(query);
				
				ResultSet entityTypes = (ResultSet) DatabaseUtility.executeQuerySingleConnection(query);
				
				System.out.println("Recommendations:");
				while(entityTypes.next())
				{
					ResultSet news = (ResultSet) DatabaseUtility.executeQuerySingleConnection("select id,title,description from twitter.news where id="+entityTypes.getString("newsId"));
					while(news.next()){
					System.out.println("=====" +news.getString(1)+"=====");
					System.out.println("TITLE:" + news.getString(2));
					System.out.println(">:" + news.getString(3).substring(0, 50));}

				}
			}
		}


		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	/*TOPIC*/
	public void loadUserSimilarityTopicProfile()
	{

	}	

	public void loadTopicForSimilarUsers(long userId,Timestamp from,Timestamp to)
	{
		try
		{
			TreeMap<Double,String>map = loadUserSimilarityEntityProfile(userId);
			long sim_user;
			String list=null;
			int count=0;
			for(Map.Entry<Double,String>  entry : map.entrySet())
			{
				if(count>6) break;
				count++;
				sim_user = Long.valueOf(entry.getValue());				
				System.out.println("**** Similar User  "+sim_user + "******");				


				String query ="SELECT newsId FROM twitter.semanticsnewsTopic where publish_date >=\""+from
						+"\" AND publish_date <=\""+to+ "\""+ 
						"order by relevance desc limit 0,3";
				//System.out.println(query);
				
				ResultSet entityTypes = (ResultSet) DatabaseUtility.executeQuerySingleConnection(query);
				
				System.out.println("Recommendations:");
				while(entityTypes.next())
				{
					ResultSet news = (ResultSet) DatabaseUtility.executeQuerySingleConnection("select id,title,description from twitter.news where id="+entityTypes.getString("newsId"));
					while(news.next()){
					System.out.println("=====" +news.getString(1)+"=====");
					System.out.println("TITLE:" + news.getString(2));
					System.out.println(">:" + news.getString(3).substring(0, 50));}
				}
			}
		}


		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	/*HASHTAG*/
	public void loadUserSimilarityHashProfile()
	{

	}

	public static void main(String[] args) 
	{
		Timestamp profileFrom = Timestamp.valueOf("2010-7-15 00:00:00");		
		Timestamp profileTo = Timestamp.valueOf("2010-12-29 00:00:00");

		new ReccomendationEngine().loadEntityForSimilarUsers(106283401,profileFrom, profileTo);
		
		//new ReccomendationEngine().loadTopicForSimilarUsers(106283401,profileFrom, profileTo);
		
	}

}
