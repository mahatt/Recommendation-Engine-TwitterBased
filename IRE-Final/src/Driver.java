import java.io.File;
import java.io.FileWriter;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import processing.CosineSimilarity;
import processing.HashTagProcessor;




public class Driver 
{



	public static void main(String args[])
	{
		int userId=106283401;
		Timestamp profileFrom = Timestamp.valueOf("2010-7-15 00:00:00");		
		Timestamp profileTo = Timestamp.valueOf("2010-12-29 00:00:00");

		if(args.length==3)
		{
			userId = Integer.parseInt(args[0]);
			profileFrom = Timestamp.valueOf(args[1]+" "+args[2]);
			profileTo = Timestamp.valueOf(args[3]+" "+args[4]);
		}

		
		System.out.println("##### HashTag Based Recommendation #####");
		new HashTagBasedRecommender(userId).collectHashTagsForUser();
		System.out.println("##### Entity Based Recommendation #####");
		new ReccomendationEngine().loadEntityForSimilarUsers(userId,profileFrom, profileTo);
		System.out.println("##### Topic Based Recommendation #####");
		new ReccomendationEngine().loadTopicForSimilarUsers(userId,profileFrom, profileTo);

	}











	public static void main_run(String[] args)
	{
		CosineSimilarity.RunSimilarityForProfiles();
	}

	public static void hash_main(String[] args) 
	{
		HashTagProcessor htp = new HashTagProcessor();
		//htp.crateHashTagSetForAll();

		/*		htp.createHashTagVectorForUser(215922210,htp.loadHashDimensionList(), 10);
		htp.createHashTagVectorForUser(209814187,htp.loadHashDimensionList(), 10);
		 */		htp.createHashTagVectorForAll();
		 //LinkedHashMap<String,Integer>map=htp.loadHashDimension();	

		 //System.out.println(map.get("WikiLeaks"));
	}

}
