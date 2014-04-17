package utility;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.object.BatchSqlUpdate;
import org.springframework.jdbc.object.SqlUpdate;

public class DatabaseUtility 
{
	public static DriverManagerDataSource ds = new DriverManagerDataSource();

	/*DataBAse connection information*/	
	static
	{
		ds.setDriverClassName("com.mysql.jdbc.Driver");
		ds.setUrl("jdbc:mysql://localhost/twitter");
		ds.setUsername("root");
		ds.setPassword("root");
	}

	public static Connection conn = null;


	/*Executes single query on dataset Wrapper*/
	public static final ResultSet executeQuery(String query)
	{
		return executeQuery(query, ds);
	}


	private static final ResultSet executeQuery(String query, DriverManagerDataSource datasource)
	{
		Statement sqlStatement = null;
		try
		{
			sqlStatement = datasource.getConnection().createStatement();
			return sqlStatement.executeQuery(query);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public static final ResultSet executeQuerySingleConnection(String query)
	{
		return executeQuerySingleConnection(query, ds);
	}
	
	public static final ResultSet executeQuerySingleConnection(String query, DriverManagerDataSource datasource)
	{
		Statement sqlStatement = null;
		try
		{
			if (conn == null) 
			{
				conn = datasource.getConnection();
			}
			sqlStatement = conn.createStatement();
			return sqlStatement.executeQuery(query);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		return null;
	}
	
	/**USER ID related procs ***/
	

	/*** HASH TAG DB PROCs***/
	public static void storeHashTagPerUser(long tweetId, long userId, String hashtag, Timestamp creationTime)
	{
		try
		{
			SqlUpdate su = new SqlUpdate
					(ds, 
							"INSERT INTO twitter.has ( tweetId, userId, hashtag, creationTime )  values (?,?,?,?)");

			su.declareParameter(new SqlParameter("tweetId", -5));
			su.declareParameter(new SqlParameter("userId", -5));
			su.declareParameter(new SqlParameter("hashtag", 12));
			su.declareParameter(new SqlParameter("creationTime", 93));

			su.compile();

			su.update(new Object[] { Long.valueOf(tweetId), Long.valueOf(userId), hashtag, creationTime });
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/****/
	
}
