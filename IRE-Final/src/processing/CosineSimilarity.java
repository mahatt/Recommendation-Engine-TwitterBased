package processing;

import java.io.File;
import java.io.FileWriter;

import org.apache.mahout.math.matrix.DoubleMatrix1D;

public class CosineSimilarity 
{

	public static void RunSimilarityForProfiles()
	{
		File folder  = new File("D:\\Dataset\\HashProfileVector\\");
		File[] files = folder.listFiles();
		HashTagProcessor htp = new HashTagProcessor();
		for(File x:files)
		{	

			int a_userId=Integer.parseInt(x.getName().split("\\.")[0]);
			DoubleMatrix1D a = htp.loadUserProfile(a_userId);
			File userFile = new File("D:\\Dataset\\HashProfileSimilarity\\"+a_userId+".txt");
			try
			{
				FileWriter fw = new FileWriter(userFile);
				for(File y:files)
				{
					int b_userId =Integer.parseInt(y.getName().split("\\.")[0]);
					DoubleMatrix1D b = htp.loadUserProfile(b_userId);
					fw.write(b_userId + " " +similarity(a, b)+"\n");
				}
				fw.close();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}

	public static Double similarity(DoubleMatrix1D a, DoubleMatrix1D b)
	{
		Double n = Double.valueOf(Math.sqrt(a.zDotProduct(a)) * Math.sqrt(b.zDotProduct(b)));
		if ((!n.isNaN()) && (n.doubleValue() > 0.0D)) {
			return a.zDotProduct(b) / n.doubleValue();
		}
		return 0.0D;

	}
}
