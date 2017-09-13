package smash.metatrader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import smash.api.CallbackClient;
import smash.api.DataCallback;
import smash.api.SessionEvent;
import smash.api.SessionEventCallback;

/**
 * Take CSV data and publish to Smash.bi
 */
public class SmashCSVPublisher implements SessionEventCallback
{
	private final HashMap<String,String> dataToDatasetUUIDLookup = new HashMap<String,String>();
	private final HashMap<String,Integer> dataFieldIdToDataColumnLookup = new HashMap<String,Integer>();
	private final CountDownLatch eventDisconnect = new CountDownLatch(1);
	private CallbackClient client;
	private final JsonParser jsonParser = new JsonParser();
	private String smashHost;
	private String smashVPN;
	private String smashUserId;
	private String smashPassword;
	private boolean smashLoggedIn;
	private boolean disposed;
	private String typeId;
	private String dataFile;
	private int datasetDataColumn;
	private int timestampDataColumn;
	private ZoneId newYorkTimeZone = ZoneId.of("America/New_York");
	private ZoneId utcTimeZone = ZoneId.of("UTC");
	DateTimeFormatter dateFormat = null;
	private PublishRequestResponseCallback publishRequestResponseCallback = new PublishRequestResponseCallback();
	private int publishRequestCount;
	
	/**
	 * create SmashMetaTraderPublisher
	 * @param aSmashHost smash host
	 * @param aSmashVPN VPN identifier
	 */
	public SmashCSVPublisher( String aSmashHost, String aSmashVPN, 
			String aSmashUserId, String aSmashPassword, String aTypeId, String aDataFile, int aDatasetDataColumn, int aTimestampDataColumn, String aTimestampFormat )
	{
		smashHost = aSmashHost;
		smashVPN = aSmashVPN;
		smashUserId = aSmashUserId;
		smashPassword = aSmashPassword;
		typeId = aTypeId;
		dataFile = aDataFile;
		datasetDataColumn = aDatasetDataColumn;
		timestampDataColumn = aTimestampDataColumn;
		dateFormat = DateTimeFormatter.ofPattern( aTimestampFormat ).withZone( newYorkTimeZone );
	}

	/**
	 * initialize
	 */
	public void initialize() throws Exception
	{
		client = new CallbackClient( smashHost, smashVPN, this );
		// prepare Smash.bi
System.out.println( "Logging in " + smashUserId + " " + smashPassword );
		client.login(smashUserId, smashPassword, new LoginResponseCallback());
	}
	
	/**
	 * add to subscription
	 * @param aData data
	 * @param aDatasetUUID dataset UUID
	 */
	public void addToSubscription( String aData, String aDatasetUUID )
	{
		dataToDatasetUUIDLookup.put( aData, aDatasetUUID );
	}
	
	/**
	 * add to data field to column index mapping
	 * @param aDataFieldId data
	 * @param aDatasetUUID dataset UUID
	 */
	public void addToDataFieldMapping( String aDataFieldId, Integer aColumnIndex )
	{
		dataFieldIdToDataColumnLookup.put( aDataFieldId, aColumnIndex );
	}
	
	public void onEvent(SessionEvent anEvent) 
	{
		System.out.println("SessionEvent:" + anEvent);
	}
	
	/**
	 * dispose
	 */
	public void dispose()
	{
		disposed = true;
    	System.out.println("Disconnecting...");
		try
		{
        	client.logout();
		}
		catch( Throwable t )
		{
			t.printStackTrace();
		}
		eventDisconnect.countDown();
        System.out.println("Exit");
	}
	
	/**
	 * convert quote data to JSON 
	 */
	private String toJSON( String aDatasetId, List<String> aData ) throws ParseException
	{
		StringBuilder builder = new StringBuilder();
		String datasetUUID = aDatasetId;
		builder.append( "{ \"dataset\": \"" );
		builder.append( datasetUUID );
		builder.append( "\", \"typeId\": \"" );
		builder.append( typeId );
		builder.append( "\", \"timestamp\": " );
		String dateTimeText = aData.get(timestampDataColumn);
		
		ZonedDateTime time = ZonedDateTime.parse( dateTimeText, dateFormat ).withZoneSameInstant( utcTimeZone );
		long quoteTime = time.toInstant().toEpochMilli();
		builder.append( quoteTime );
		for( String fieldId: dataFieldIdToDataColumnLookup.keySet() )
		{
			builder.append( ", \"" );
			builder.append( fieldId );
			builder.append( "\": ");
			BigDecimal value = new BigDecimal( aData.get( dataFieldIdToDataColumnLookup.get( fieldId )));
			builder.append( value.toPlainString() );
		}
		builder.append( "}" );
		return builder.toString(); 
	}
	
	/**
	 * await
	 */
	public void await()
	{
		while( !disposed )
		{
			try
			{
				eventDisconnect.await(10, TimeUnit.SECONDS);
			}
			catch( Throwable t )
			{}
		}
	}
	
	/**
	 * login response callback
	 */
	private class LoginResponseCallback implements DataCallback
	{
		public void onReceive(String aData) 
		{
System.out.println( "Receive Login Response " + aData );
			smashLoggedIn = true;
			for( String datasetUUID :dataToDatasetUUIDLookup.values())
			{
				try
				{
					client.publishDataRequest(datasetUUID, publishRequestResponseCallback );
				}
				catch( Throwable t )
				{
					t.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * login response callback
	 */
	private class PublishRequestResponseCallback implements DataCallback
	{
		public void onReceive(String aData) 
		{
System.out.println( "Receive Publish Response " + aData );
			JsonElement jsonElement = jsonParser.parse(aData);
			JsonObject jsonObject = jsonElement.getAsJsonObject();
			if ( "SUCCESS".equals( jsonObject.get( "status" ).getAsString() ) )
			{
				publishRequestCount++;
				if ( publishRequestCount == dataToDatasetUUIDLookup.size() )
				{
					publish();
				}
			}
		}
	}
	
	public void publish()
	{
		FileReader fileReader = null;
		BufferedReader reader = null;
		try
		{
			fileReader = new FileReader( dataFile );
			reader = new BufferedReader( fileReader );
			String line = null;
			while( ( line = reader.readLine() ) != null )
			{
				List<String> data = Arrays.asList(line.split("\\s*,\\s*"));
				String datasetUUID = dataToDatasetUUIDLookup.get( data.get(datasetDataColumn) );
				if ( datasetUUID != null )
				{
					String jsonData = toJSON(datasetUUID, data);
System.out.println( "PUBLISH");
System.out.println( jsonData );
					client.publishData(datasetUUID, jsonData);
				}
			}
		}
		catch( Throwable t )
		{
			t.printStackTrace();
		}
		finally
		{
			try
			{
				fileReader.close();
			}
			catch( Throwable tt )
			{}
			try
			{
				reader.close();
			}
			catch( Throwable tt )
			{}
			fileReader = null;
			reader = null;
			dispose();
		}
	}
	
    public static void main( String[] args )
    {
    	try
    	{
    		Properties properties = new Properties();
    		properties.load( new FileInputStream(args[0]));
    		SmashCSVPublisher publisher = new SmashCSVPublisher( 
    												properties.getProperty( "smash.csvPublisher.smashHost" ),
    												properties.getProperty( "smash.csvPublisher.smashVPN" ),
    												properties.getProperty( "smash.csvPublisher.smashUserId" ),
    												properties.getProperty( "smash.csvPublisher.smashPassword" ),
    												properties.getProperty( "smash.csvPublisher.smashDataTypeId" ),
    												properties.getProperty( "smash.csvPublisher.dataFile" ),
    												Integer.parseInt(properties.getProperty( "smash.csvPublisher.datasetDataColumn")),
    												Integer.parseInt(properties.getProperty( "smash.csvPublisher.timestampDataColumn")),
    												properties.getProperty( "smash.csvPublisher.timestampFormat" ));
    		StringTokenizer subscriptionMappingConfig = new StringTokenizer( 
    											properties.getProperty( "smash.csvPublisher.subscriptionMapping" ), "," );
    		
    		String symbol = null;
    		while( subscriptionMappingConfig.hasMoreTokens())
    		{
    			if ( symbol == null )
    			{
    				symbol = subscriptionMappingConfig.nextToken();
    			}
    			else
    			{
    				publisher.addToSubscription( symbol, subscriptionMappingConfig.nextToken() );
    				symbol = null;
    			}
    		}
    		StringTokenizer dataFieldMappingConfig = new StringTokenizer( 
					properties.getProperty( "smash.csvPublisher.dataFieldMapping" ), "," );
    		String dataFieldId = null;
    		while( dataFieldMappingConfig.hasMoreTokens())
    		{
    			if ( dataFieldId == null )
    			{
    				dataFieldId = dataFieldMappingConfig.nextToken();
    			}
    			else
    			{
    				publisher.addToDataFieldMapping( dataFieldId, Integer.parseInt(dataFieldMappingConfig.nextToken()) );
    				dataFieldId = null;
    			}
    		}
    		publisher.initialize();
    		publisher.await();
    	}
    	catch( Throwable t )
    	{
    		t.printStackTrace();
    	}
    }
}
