import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.mahout.math.matrix.DoubleMatrix1D;
import org.apache.mahout.math.matrix.impl.DenseDoubleMatrix1D;


import utility.DatabaseUtility;



import enumrators.UM_TimeSlot;


public class HashTagUserProfiler 
{

	
	public static Map<String, Integer> getHashtagDimension()
	{
		Map<String, Integer> hashtagDimension = null;
		
		if (hashtagDimension == null)
		{
			hashtagDimension = new LinkedHashMap();
			ResultSet rs = DatabaseUtility.executeQuerySingleConnection("SELECT h.hashtag, count(distinct h.userid) n FROM has h, user_samples u WHERE h.userid = u.userid AND u.sample = \"umap-20t-10i\" GROUP BY h.hashtag ORDER BY n DESC LIMIT 0, 10000");
		
			int counter = 0;
			try
			{
				while (rs.next())
				{
					hashtagDimension.put(rs.getString("hashtag"), Integer.valueOf(counter));
					counter++;
				}
			}
			catch (SQLException e)
			{
				System.err.println("ERROR OCCURED WHEN TRYING TO FETCH A ROW FOR CREATING HASHTAGDIMENSION.");
				e.printStackTrace();
			}
			return hashtagDimension;
		}
		return hashtagDimension;
	}

	public static DoubleMatrix1D getHashtagBasedProfileViaTweets(Integer userId, Timestamp from, Timestamp to, UM_TimeSlot timeSlot, int minProfileSize, Set<Long> tweetsToExclude)
	{
		DoubleMatrix1D profile = new DenseDoubleMatrix1D(getHashtagDimension().size());

		String timeSlotCondition = getTimeSlotConditionTweets(timeSlot);
		
		ResultSet hashtags = DatabaseUtility.executeQuerySingleConnection("SELECT hashtag, count(distinct tweetId) n from has WHERE userId = " + 

       userId + " AND creationTime >= \"" + from + "\" AND creationTime <= \"" + to + "\" " + 
       timeSlotCondition + "GROUP BY hashtag");
		
		try
		{
		while(hashtags.next())
		{
			System.out.println(hashtags.getString("hashtag") + " " +hashtags.getInt("n"));
		}
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		return profile;
	}
	

	
	
	/**UTILITY FINCTIONS*/
	public static String getTimeSlotConditionTweets(UM_TimeSlot timeSlot)
	{
		String timeSlotCondition = "";
		if (timeSlot == UM_TimeSlot.All) {
			timeSlotCondition = "";
		} else if (UM_TimeSlot.Weekdays.equals(timeSlot)) {
			timeSlotCondition = "AND (DAYOFWEEK(creationTime) != 1 AND DAYOFWEEK(creationTime) != 7) ";
		} else if (UM_TimeSlot.Weekends.equals(timeSlot)) {
			timeSlotCondition = "AND (DAYOFWEEK(creationTime) = 1 OR DAYOFWEEK(creationTime) = 7) ";
		} else if (UM_TimeSlot.Day.equals(timeSlot)) {
			timeSlotCondition = "AND (HOUR(creationTime) >= 9 AND HOUR(creationTime) <= 17 ) ";
		} else if (UM_TimeSlot.Night.equals(timeSlot)) {
			timeSlotCondition = "AND (HOUR(creationTime) >= 18 OR HOUR(creationTime) <= 3) ";
		} else if (UM_TimeSlot.MonTue.equals(timeSlot)) {
			timeSlotCondition = "AND (DAYOFWEEK(creationTime) = '2' or DAYOFWEEK(creationTime) = '3') ";
		} else if (UM_TimeSlot.WedThur.equals(timeSlot)) {
			timeSlotCondition = "AND (DAYOFWEEK(creationTime) = '4' or DAYOFWEEK(creationTime) = '5') ";
		} else if (UM_TimeSlot.DayEvenHours.equals(timeSlot)) {
			timeSlotCondition = "AND (HOUR(creationTime) = '10' or HOUR(creationTime)='12' or HOUR(creationTime)='14' or HOUR(creationTime)='16') ";
		} else if (UM_TimeSlot.DayOddHours.equals(timeSlot)) {
			timeSlotCondition = "AND (HOUR(creationTime) = '9' or HOUR(creationTime)='11' or HOUR(creationTime)='13' or HOUR(creationTime)='15') ";
		} else if (UM_TimeSlot.NightEvenHours.equals(timeSlot)) {
			timeSlotCondition = "AND (HOUR(creationTime) = '20' or HOUR(creationTime)='22' or HOUR(creationTime)='0' or HOUR(creationTime)='2') ";
		} else if (UM_TimeSlot.NightOddHours.equals(timeSlot)) {
			timeSlotCondition = "AND (HOUR(creationTime) = '19' or HOUR(creationTime)='21' or HOUR(creationTime)='23' or HOUR(creationTime)='1') ";
		}
		return timeSlotCondition;
	}

}
