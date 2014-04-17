import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.LinkedHashMap;

import org.apache.mahout.math.matrix.DoubleMatrix1D;
import org.apache.mahout.math.matrix.impl.DenseDoubleMatrix1D;

import processing.EntityProcessing;

import utility.DatabaseUtility;


public class NewsProfiler 
{

	public DoubleMatrix1D getEntityNewsProfile(long userId,Timestamp from,Timestamp to,LinkedHashMap<String,Integer>entityDimension)
	{
		DoubleMatrix1D newsprofile = new DenseDoubleMatrix1D(entityDimension.size());
		double sum = 0.0D;

		try
		{
			ResultSet entityTypes = 
					DatabaseUtility.executeQuerySingleConnection("SELECT name, count(distinct newsId) n from semanticsNewsEntity WHERE publish_date >= \"" + 
							from + 
							"\" AND publish_date <= \"" + 
							to + 
							"\" " + 
							"   AND newsId in (" + 
							getNewsIdsRelatedToUserAsString(userId) 
							+") " + " GROUP BY name");


			if (entityTypes != null) 
			{
				while (entityTypes.next())
				{
					newsprofile.set(((Integer)entityDimension.get(entityTypes.getString("name"))).intValue(), entityTypes.getDouble("n"));
					sum += entityTypes.getDouble("n");
				}
			}
			if (sum == 0.0D)
			{
				newsprofile = null;
			}
			else
			{
				Double current = Double.valueOf(0.0D);
				for (int i = 0; i < newsprofile.size(); i++)
				{
					current = Double.valueOf(newsprofile.get(i));
					System.out.println(current);
					if (current.isNaN()) 
					{
						newsprofile.set(i, 0.0D);
					} 
					else 
					{
						newsprofile.set(i, current.doubleValue() / sum);
					}
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		return newsprofile;
	}

	public String getNewsIdsRelatedToUserAsString(long userId)
	{

		ResultSet newsIds = 
				DatabaseUtility.executeQuerySingleConnection("SELECT distinct nas.newsId FROM nas, tweets t WHERE t.userId = " + 
						userId + 
						" AND nas.tweetId = t.id ");
		String relatedNewsIds = null;
		int counter = 0;
		try
		{
			while (newsIds.next())
			{
				if (relatedNewsIds == null) 
				{
					relatedNewsIds = newsIds.getString(1);
				} else 
				{
					relatedNewsIds = relatedNewsIds + "," + newsIds.getString(1);
				}
				counter++;
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return relatedNewsIds;
	}


	public static void main(String args[])
	{
		Timestamp profileFrom = Timestamp.valueOf("2010-11-15 00:00:00");		
		Timestamp profileTo = Timestamp.valueOf("2010-12-29 00:00:00");
		
		EntityProcessing ep = new EntityProcessing();
		System.out.println(new NewsProfiler().getEntityNewsProfile(21985529, profileFrom, profileTo, ep.entityDimention));
		//System.out.println(new NewsProfiler().getNewsIdsRelatedToUserAsString(21985529));
	}

}
