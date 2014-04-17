package processing;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.mahout.math.matrix.DoubleMatrix1D;
import org.apache.mahout.math.matrix.impl.DenseDoubleMatrix1D;

import utility.DatabaseUtility;

public class EntityProcessing 
{
	public LinkedHashMap<String,Integer> entityDimention=null;

	public EntityProcessing()
	{
		entityDimention = (LinkedHashMap<String, Integer>) getDimension();
	}


	public Map<String,Integer> getDimension()
	{
		Map<String,Integer> map = new LinkedHashMap<String, Integer>();

		ResultSet rs = DatabaseUtility.executeQuerySingleConnection("SELECT distinct type FROM twitter.semanticstweetsentity  limit 0,500;");
		try
		{ 
			int count=0;
			while(rs.next())
			{
				map.put(rs.getString("type"),Integer.valueOf(count++));
			}

			//System.out.println(map.size());
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return map;
	}

	@SuppressWarnings("deprecation")
	public DoubleMatrix1D getProfile(long userId, Timestamp from, Timestamp to)
	{

		DoubleMatrix1D profile = new DenseDoubleMatrix1D(entityDimention.size());

		double sum = 0.0D;
		ResultSet entityTypes = DatabaseUtility.executeQuerySingleConnection("SELECT type, count(distinct tweetId) n from semanticsTweetsEntity WHERE userId = " + 
				userId 
				+" AND creationTime >= \""+ from 
				+"\" AND creationTime <= \"" + to + "\" "  
				+ "GROUP BY type");

		try
		{
			if(entityTypes!=null)
			{
				while(entityTypes.next())
				{
					profile.set((Integer)entityDimention.get(entityTypes.getString("type")).intValue(), entityTypes.getDouble("n"));
					sum += entityTypes.getDouble("n");
				}
			}
			
			// Smoothing
			
			if (sum == 0.0D)
			{
				
				profile = new DenseDoubleMatrix1D(new double[] 
						{ 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 
						  0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 
						  0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 
						  0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D });
			}
			else
			{
				Double current = Double.valueOf(0.0D);
				for (int i = 0; i < profile.size(); i++)
				{

					current = Double.valueOf(profile.get(i));
					//System.out.println(current);
					if (current.isNaN()) 
					{
						profile.set(i, 0.0D);
					} else 
					{
						profile.set(i, current.doubleValue() / sum);
					}
				}
			}
			
			//end of smoothing
	
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		return profile;
	}

	
	
	
		public static void main(String args[])
	{
		Timestamp profileFrom = Timestamp.valueOf("2010-7-15 00:00:00");		
		Timestamp profileTo = Timestamp.valueOf("2010-12-29 00:00:00");
		
		DoubleMatrix1D profile= new EntityProcessing().getProfile(21985529,profileFrom,profileTo);
		
	}

}
