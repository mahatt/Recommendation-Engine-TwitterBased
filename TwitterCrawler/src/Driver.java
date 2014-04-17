import Crawler.TwitterCrawler;


public class Driver 
{

	public static void main(String[] args) 
	{
		TwitterCrawler crawler = new TwitterCrawler();
		//crawler.testRun();

		crawler.testTweetFetch();
	}

}
