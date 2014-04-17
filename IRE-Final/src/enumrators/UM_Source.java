package enumrators;

public enum UM_Source
{
	Twitter_based,  News_based,  Twitter_and_News_based;

	public String toString()
	{
		if (Twitter_based.equals(this)) 
		{
			return "twitter";
		}
		
		if (News_based.equals(this)) 
		{
			return "news";
		}
		
		return "twitter-news";
	}
}