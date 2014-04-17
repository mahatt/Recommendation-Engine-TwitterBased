import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.TreeMap;

import model.WeightedItem;

import org.apache.mahout.math.matrix.DoubleMatrix1D;

import processing.HashTagProcessor;


public class HashTagBasedRecommender 
{
		int userId;

		final String hashprofilefolder="D:\\Dataset\\HashProfileSimilarity\\";
		final String hashprofilevector="D:\\Dataset\\HashProfileVector\\";
		public HashTagBasedRecommender(int userId)
		{
			this.userId=userId;
		}
		
		public void collectHashTagsForUser()
		{
			HashTagProcessor htp = new HashTagProcessor();
			ArrayList<Long> sim_users = MatchingProfiles();
			LinkedList<String> present_tags=htp.loadHashDimensionList();
			
			TreeMap<Long,String> suggestionlist = new TreeMap<Long,String>();
			
			System.out.println("HashTag Recommendation:");
			for(Long user : sim_users)
			{
					DoubleMatrix1D userDimesion  = htp.loadUserProfile(user);
						int count=0;

					for(int i=0;i< userDimesion.size();i++)
					{
						Double weight=userDimesion.get(i);
						String hashtag=present_tags.get(i);
						///// stopped
						if(count>2) break;
						count++;
						if(weight>0.0D) 
						{							
							System.out.println(hashtag);
						}
					}
			}
			
			
		}
		
		public ArrayList<Long> MatchingProfiles()
		{
			ArrayList<WeightedItem<Double>> list= loadUserVector();
			ArrayList<Long> sim_users= new ArrayList<Long>();
			
			
			for(int i=1;i<6;i++)
			{
				//System.out.println(list.get(i).id+" "+list.get(i).weight);
				sim_users.add(list.get(i).id);				
			}
			
			return sim_users;
		}
		
		public ArrayList<WeightedItem<Double>> loadUserVector()
		{
			ArrayList<WeightedItem<Double>> cos_sim = new ArrayList<WeightedItem<Double>>();
			
			try
			{
				BufferedReader breader = new BufferedReader(new FileReader(new File(hashprofilefolder+userId+".txt")));
				String line= breader.readLine();
				
				String array[]=null;
				long id=0;
				Double weight=0.0D;
				
				while(line!=null)
				{
					array = line.split(" ");
					
					id= Long.parseLong(array[0]);
					weight = Double.parseDouble(array[1]);
					
					cos_sim.add(new WeightedItem<Double>(id, weight));
					
					line= breader.readLine();
				}
				
				// SORT
				
				Collections.sort(cos_sim,new Comparator<WeightedItem<Double>>() 
				{
				       public int compare(WeightedItem<Double> o1, WeightedItem<Double> o2)
				       {
				         return ((Double)o1.weight).compareTo((Double)o2.weight) * -1;
				       }

				});
				
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			
			return cos_sim;
		}
		
		
		
		public static void main(String[] args)
		{
			new HashTagBasedRecommender(106283401).collectHashTagsForUser();
		}
}
