package processing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.mahout.math.matrix.DoubleMatrix1D;
import org.apache.mahout.math.matrix.impl.DenseDoubleMatrix1D;
import org.apache.mahout.math.matrix.impl.SparseDoubleMatrix1D;

import utility.DatabaseUtility;

public class HashTagProcessor 
{	
	public HashTagProcessor() 
	{

	}

	public DoubleMatrix1D loadUserProfile(long userId)
	{
		try
		{

			FileReader  fr = new FileReader(new File("D:\\Dataset\\HashProfileVector\\"+userId+".txt"));
			Scanner s = new Scanner(fr);
			
			DoubleMatrix1D profile = new DenseDoubleMatrix1D(10000);
			int count=0;
			while(s.hasNext())
			{
				
				profile.set(count, s.nextDouble());
				//System.out.println(s.nextDouble());
				count++;
			}
			//System.out.println(count);
			return profile;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return null;
	}
	
	public LinkedList<String> loadHashDimensionList()
	{
		try
		{
			BufferedReader breader = new BufferedReader(new FileReader(new File("D:\\Dataset\\processing\\hashtag_userscount.txt")));
			String line=null;
			String key=null;
			int value=0;

			LinkedList<String> map = new LinkedList<String>();
			line=breader.readLine();
			while (line!=null)
			{

				String[] segments = line.split("\\s+");
				key=segments[0];
				if(key==null)
				{
					System.out.println("Null Key");
					return map;
				}	
				try
				{
					value=Integer.parseInt(segments[1]);
				}
				catch(NumberFormatException e)
				{
					e.printStackTrace();
				}
				
				map.add(key);
				line=breader.readLine();
			}
			//System.out.println(map.size());
			return map;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return null;
		
	}
	
	// #######################   DO NOT USE #########################
	public LinkedHashMap<String,Integer> loadHashDimension()
	{
		try
		{
			BufferedReader breader = new BufferedReader(new FileReader(new File("D:\\Dataset\\processing\\hashtag_userscount.txt")));
			String line=null;
			String key=null;
			int value=0;

			LinkedHashMap<String,Integer> map = new LinkedHashMap<String,Integer>();
			line=breader.readLine();
			while (line!=null)
			{

				String[] segments = line.split("\\s+");
				key=segments[0];
				if(key==null)
				{
					System.out.println("Null Key");
					return map;
				}	
				try
				{
					value=Integer.parseInt(segments[1]);
				}
				catch(NumberFormatException e)
				{
					e.printStackTrace();
				}
				
				map.put(key,value);
				line=breader.readLine();
			}
			//System.out.println(map.size());
			return map;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		return null;
	}

	public void createHashTagVectorForAll()
	{

		ResultSet rs = DatabaseUtility.executeQuery("select  userId from twitter.user_samples LIMIT 0, 1500;");
		long userId = -1L;
		try
		{
			//LinkedHashMap<String,Integer> map = loadHashDimension();
			LinkedList<String> map = loadHashDimensionList();
			
			while(rs.next())
			{
				userId=rs.getLong("userId");
				System.out.println(userId);
				createHashTagVectorForUser(userId,map,10);
			}			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}
	
	public void createHashTagVectorForUser(long userId,LinkedList<String> map,int minProfileSize)
	{
		try
		{
			DoubleMatrix1D profile = new DenseDoubleMatrix1D(map.size());
//			DoubleMatrix1D profile = new SparseDoubleMatrix1D(map.size());
			
			//System.out.println(map.size());
			double sum = 0.0D;
			for(int i=0;i<map.size();i++)
			{
				profile.set(i, 0.0D);
			}

			ResultSet rs = DatabaseUtility.executeQuery("SELECT hashtag, count(distinct tweetId) n from twitter.has WHERE userId="+userId+" group by hashtag order by hashtag asc limit 0,75000; ");

			int notCount=0,isCount=0;;
			while(rs.next())
			{
				if(map.contains(rs.getString("hashtag")))
				{
					isCount++;
					profile.set(map.indexOf(rs.getString("hashtag")), rs.getDouble("n"));					 
					sum += rs.getDouble("n");
				}
				else
				{
					notCount++;
				}	
			}
			System.out.print(" " +notCount +" " +isCount);
			if ((sum < new Integer(minProfileSize).doubleValue()) || (sum == 0.0D))
			{
				//System.out.println(sum);			
				profile = null;
			}
			else
			{
				Double current = Double.valueOf(0.0D);
				System.out.println(profile.size());
				for (int i = 0; i < profile.size(); i++)
				{
					current = Double.valueOf(profile.get(i));
					//System.out.println(current);
					if (current.isNaN()) 
					{
						profile.set(i, 0.0D);
					} 
					else 
					{
						profile.set(i, current.doubleValue() / sum);
					}
				}

			}

			// Write To File
			if(profile==null)
				return;
			FileWriter fw = new FileWriter(new File("D:\\Dataset\\HashProfileVector\\"+userId+".txt"));
			int i=0;
			for(i=0;i<profile.size();i++)
			{
				try
				{
					fw.write(Double.valueOf(profile.get(i)).toString()+"\n");
				}
				catch(IOException e)
				{
					e.printStackTrace();
				}
			}
			fw.close();
			//System.out.println("NINJA:"+i);
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	// #######################   DO NOT USE #########################
	public void createHashTagVectorForUser(long userId,LinkedHashMap<String,Integer> map,int minProfileSize)
	{
		try
		{
			DoubleMatrix1D profile = new DenseDoubleMatrix1D(map.size());

			double sum = 0.0D;

			ResultSet rs = DatabaseUtility.executeQuery("SELECT hashtag, count(distinct tweetId) n from twitter.has WHERE userId="+userId+" group by hashtag order by hashtag asc limit 0,75000; ");


			while(rs.next())
			{
				if(map.containsKey(rs.getString("hashtag")))
				{
					profile.set(((Integer)map.get(rs.getString("hashtag"))).intValue(), rs.getDouble("n"));					 
					sum += rs.getDouble("n");
				}				
			}

			if ((sum < new Integer(minProfileSize).doubleValue()) || (sum == 0.0D))
			{
				System.out.println(sum);
			
				profile = null;
			}
			else
			{
				Double current = Double.valueOf(0.0D);

				for (int i = 0; i < profile.size(); i++)
				{
					current = Double.valueOf(profile.get(i));
					if (current.isNaN()) 
					{
						profile.set(i, 0.0D);
					} 
					else 
					{
						profile.set(i, current.doubleValue() / sum);
					}
				}

			}

			// Write To File

			FileWriter fw = new FileWriter(new File("D:\\Dataset\\HashProfileVector\\"+userId+".txt"));

			for(int i=0;i<profile.size();i++)
			{
				try
				{
					fw.write((int) profile.get(i));
				}
				catch(IOException e)
				{
					e.printStackTrace();
				}
			}


		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public void crateHashTagSetForAll()
	{
		ResultSet rs = DatabaseUtility.executeQuery("select  userId from twitter.users_sample LIMIT 0, 1500;");
		long userId = -1L;
		try
		{ 			
			while(rs.next())
			{
				userId=rs.getLong("userId");
				System.out.println(userId);
				createHashTagSetForUser(userId);
			}			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public void createHashTagSetForUser(long requestUserId)
	{
		ResultSet rs = DatabaseUtility.executeQuery("select  * from twitter.tweets_sample where userId="+requestUserId+";");

		/*Hash Tag processing */
		Pattern pattern = Pattern.compile("#[\\w]+");
		Matcher matcher=null;

		/*Database for hashtag for user*/
		long tweetId = -1L;
		long userId = -1L;
		String content = null;
		Timestamp timestamp = null;

		try
		{
			int count=0;
			while(rs.next())
			{
				content = rs.getString("content");
				matcher = pattern.matcher(content);

				while(matcher.find())
				{
					count++;
					String hashtag=matcher.group().trim().substring(1);
					DatabaseUtility.storeHashTagPerUser(rs.getLong("id"),rs.getLong("userId"),hashtag,rs.getTimestamp("creationTime"));
				}


			}
			System.out.println(count);
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}

	}


}
