package Crawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterCrawler 
{

	final String API_Key="sriblPOjHFzXxiMvvqSghw";
	final String API_Secret="DrpmjLWjcE6tbt42lq17g3f7JTWW174K0DlOgKmUrgU";
	final String Access_Token="257113807-nsZ7B5FCjCYCMuSG3AS5wgTbdwUZ3x2esgJBHN7P";
	final String Access_Token_Secret="Vg3KX7ppEGHUGHrcDgUG6gZcMdWbeFEY2V7dVTSNissAQ";
	
	public void testTweetFetch()
	{
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
			.setOAuthConsumerKey(API_Key)
			.setOAuthConsumerSecret(API_Secret)
			.setOAuthAccessToken(Access_Token)
			.setOAuthAccessTokenSecret(Access_Token_Secret);
		cb.setHttpConnectionTimeout(50000000);
		TwitterFactory tf= new TwitterFactory(cb.build());
		
		Twitter twitter = tf.getInstance();
		
		
		Query query  = new Query("from: *");
		query.setSince("2014-03-02");
		query.setUntil("2014-03-03");
		
		try
		{
			QueryResult result = twitter.search(query);
			
			while(result!=null)
			{
			System.out.println("Count : " + result.getTweets().size());
			for (Status tweet : result.getTweets()) 
			{
					System.out.println("Id : " + tweet.getId());
					System.out.println("Lang : " + tweet.getIsoLanguageCode());
					System.out.println("ReTweetCount:"+tweet.getRetweetCount());
					System.out.println("Tweet : " + tweet.getText());
			}
				 
			}
			System.out.println("Done! ");
		}
		catch(TwitterException te)
		{
			te.printStackTrace();
		}
	}
	
	public void testRun()
	{
		Twitter twitter = TwitterFactory.getSingleton();

		twitter.setOAuthConsumer(API_Key, API_Secret);
		try 
		{
			RequestToken requestToken = twitter.getOAuthRequestToken();
			AccessToken accessToken = null;

			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

			while (null == accessToken) 
			{
				System.out.println("Open the following URL and grant access to your account:");
				System.out.println(requestToken.getAuthorizationURL());
				System.out.print("Enter the PIN(if aviailable) or just hit enter.[PIN]:");
				String pin = br.readLine();
				try
				{
					if(pin.length() > 0)
					{
						accessToken = twitter.getOAuthAccessToken(requestToken, pin);
					}
					else
					{
						accessToken = twitter.getOAuthAccessToken();
					}
				} 
				catch (TwitterException te) 
				{
					if(401 == te.getStatusCode())
					{
						System.out.println("Unable to get the access token.");
					}
					else
					{
						te.printStackTrace();
					}
				}
			}
			
			

		} 
		catch (TwitterException e) 
		{
			e.printStackTrace();
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		}

	}

}
