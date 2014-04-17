package processing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.ResultSet;
import java.sql.Timestamp;

import org.apache.mahout.math.matrix.DoubleMatrix1D;
import org.apache.mahout.math.matrix.impl.DenseDoubleMatrix1D;

import utility.DatabaseUtility;


public class TopicRecommendation
{

	static TopicProcessing ep = new TopicProcessing();
	
	String profileFolder ="D:\\Dataset\\TopicProfileVector-Year\\";
	String sim_folder ="D:\\Dataset\\TopicProfileSimilarity-Year\\";

	public  void similarity()
	{
		File folder  = new File(profileFolder);

		DoubleMatrix1D X,Y;
		long x_user,y_user;
		
		try
		{
			for(String x :folder.list())
			{
				x_user =Long.parseLong(x.substring(0,x.indexOf('.')));
				X = loadProfileVector(x_user);

				FileWriter fwriter = new FileWriter(new File(sim_folder+x_user+".txt"));
				
				for(String y :folder.list())
				{
					y_user=Long.parseLong(y.substring(0,y.indexOf('.')));
					Y=loadProfileVector(y_user);
					
					Double sim = CosineSimilarity.similarity(X, Y);
					
					fwriter.write(y_user + " " + sim+"\n");
				}
				
				fwriter.close();
				
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public DoubleMatrix1D loadProfileVector(long userId)
	{
		DoubleMatrix1D profile = new DenseDoubleMatrix1D(39);
		
		try
		{
			BufferedReader breader = new BufferedReader(new FileReader(new File(profileFolder+userId+".txt")));
			
			for(int i=0;i<18;i++)
			{
				profile.set(i,Double.valueOf(breader.readLine()));
			}
			
			
	/*		for(int i=0;i<39;i++)
			{
				System.out.println(Double.valueOf(profile.get(i)));				
			}*/
		
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return profile;
	}


	public void createEntityVectorForUser(long userID)
	{
//		String entitypath ="D:\\Dataset\\TopicProfileVector-Year\\";
		String entitypath ="D:\\Dataset\\TopicProfileVector-Month\\";		
		try
		{
			
			//Timestamp profileFrom = Timestamp.valueOf("2009-12-1 00:00:00");		
			Timestamp profileTo = Timestamp.valueOf("2010-11-30 00:00:00");
			
			Timestamp XprofileTo = Timestamp.valueOf("2010-01-01 00:00:00");
			DoubleMatrix1D profile = ep.getProfile(userID,profileTo,XprofileTo);			
			//DoubleMatrix1D profile = ep.getProfile(userID,profileFrom,profileTo);

			File file  = new File(entitypath+userID+".txt");
			FileWriter fwriter = new FileWriter(file);
			
			for(int i=0;i<profile.size();i++)
			{
				fwriter.write(Double.valueOf(profile.get(i))+"\n");	
			}
		
			fwriter.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}
	
	public void createEntityVectorForAll()
	{

		ResultSet rs = DatabaseUtility.executeQuery("select  userId from twitter.user_samples LIMIT 0, 1500;");
		long userId = -1L;
		try
		{
			
			while(rs.next())
			{
				userId=rs.getLong("userId");
				createEntityVectorForUser(userId);
			}			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}


	
	public static void main(String[] args)
	{
		
		TopicRecommendation epx = new TopicRecommendation();
		//epx.createEntityVectorForAll();
		epx.similarity();
		
	}
}
