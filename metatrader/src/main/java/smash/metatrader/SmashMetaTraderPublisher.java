package smash.metatrader;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import smash.api.CallbackClient;
import smash.api.DataCallback;
import smash.api.SessionEvent;
import smash.api.SessionEventCallback;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.pretty_tools.dde.client.DDEClientConversation;
import com.pretty_tools.dde.client.DDEClientEventListener;

/**
 * Subscribe to Meta Trader quotes and publish to Smash.bi
 */
public class SmashMetaTraderPublisher implements SessionEventCallback 
{
	private static long daylightSavingTimeOffset = 0L;
	private static long standardTimeOffset = 0L;
	private final SimpleDateFormat dateFormat = new SimpleDateFormat( "yyyy/MM/dd HH:mm:ss" );
	private DDEClientConversation conversation;
	private final CountDownLatch eventDisconnect = new CountDownLatch(1);
	private final LinkedBlockingQueue<QueuedData> queue = new LinkedBlockingQueue<QueuedData>();
	private final HashMap<String,String> symbolToDatasetUUIDLookup = new HashMap<String,String>();
	private CallbackClient client;
	private final JsonParser jsonParser = new JsonParser();
	private String smashHost;
	private String smashVPN;
	private String smashUserId;
	private String smashPassword;
	private boolean smashLoggedIn;
	private boolean disposed;
	private boolean canPerformDDE;
	private String typeId;
	private long lastQuoteTimestamp = -1;
	private long lastQuoteReceivedTime = -1;
	private long lastQuoteReceivedTimeIncrements = 0;
	private TimeZone newYorkTimeZone = TimeZone.getTimeZone("America/New_York");
	
	/**
	 * create SmashMetaTraderPublisher
	 * @param aSmashHost smash host
	 * @param aSmashVPN VPN identifier
	 */
	public SmashMetaTraderPublisher( String aSmashHost, String aSmashVPN, 
			String aSmashUserId, String aSmashPassword, String aTypeId )
	{
		Runtime.getRuntime().addShutdownHook( new Thread( new ShutdownHook() ));
		canPerformDDE = System.getProperty("os.name").toLowerCase().indexOf( "win" ) >= 0;
		smashHost = aSmashHost;
		smashVPN = aSmashVPN;
		smashUserId = aSmashUserId;
		smashPassword = aSmashPassword;
		typeId = aTypeId;
	}

	/**
	 * initialize
	 */
	public void initialize() throws Exception
	{
		// prepare MT4
		if ( canPerformDDE )
		{
			conversation = new DDEClientConversation();
			conversation.setEventListener( new MetaTraderEventListener(eventDisconnect) );
			conversation.connect( "MT4", "QUOTE" );
System.out.println( "Connected to MT4" );
		}
		client = new CallbackClient( smashHost, smashVPN, this );
		Thread thread = new Thread( new PublishToSmashTask() );
		thread.start();
		// prepare Smash.bi
System.out.println( "Logging in " + smashUserId + " " + smashPassword );
		client.login(smashUserId, smashPassword, new LoginResponseCallback());
		System.out.println( "MT4 DDE started" );
	}

	/**
	 * publish all
	 */
	private void publishAll()
	{	
		for(Entry<String, String> keyValue:symbolToDatasetUUIDLookup.entrySet())
		{
			try
			{
				System.out.println( "Request publish to " + keyValue.getKey() );
				String datasetUUID = keyValue.getValue();
				client.publishDataRequest(datasetUUID, new PublishRequestResponseCallback( datasetUUID, keyValue.getKey() ) );
			}
			catch( Throwable t )
			{
				t.printStackTrace();
			}
		}		
	}
	
	/**
	 * add to subscription
	 * @param aSymbol symbol
	 * @param aDatasetUUID dataset UUID
	 */
	public void addToSubscription( String aSymbol, String aDatasetUUID )
	{
		symbolToDatasetUUIDLookup.put( aSymbol, aDatasetUUID );
		if ( smashLoggedIn)
		{
			try
			{
				System.out.println( "Subscribing to " + aSymbol );
				client.publishDataRequest(aDatasetUUID, new PublishRequestResponseCallback( aDatasetUUID, aSymbol ) );
			}
			catch( Throwable t )
			{
				t.printStackTrace();
			}			
		}
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
		try
		{
			if ( canPerformDDE )
			{
				conversation.disconnect();
			}
		}
		catch( Throwable t )
		{
			t.printStackTrace();
		}
        System.out.println("Exit");
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
	 * convert quote data to JSON 
	 */
	private String toJSON( String aDatasetId, QueuedData aData ) throws ParseException
	{
		StringBuilder builder = new StringBuilder();
		String datasetUUID = aDatasetId;
		builder.append( "{ \"dataset\": \"" );
		builder.append( datasetUUID );
		builder.append( "\", \"typeId\": \"" );
		builder.append( typeId );
		builder.append( "\", \"timestamp\": " );
		StringTokenizer dataTokenizer = new StringTokenizer( aData.data, " " );
		String dateTimeText = dataTokenizer.nextToken() + " " + dataTokenizer.nextToken();
		Date dateTime = dateFormat.parse( dateTimeText );
		long quoteTimeWithSecondPrecision = dateTime.getTime();
		
		// adjust time to UTC
		// if New York in daylight saving adjust by reducing 3hrs
		if ( newYorkTimeZone.inDaylightTime( new Date() ) )
		{
			quoteTimeWithSecondPrecision = quoteTimeWithSecondPrecision - daylightSavingTimeOffset;
		}
		// if New York NOT in daylight saving adjust by reducing 2hrs
		else
		{
			quoteTimeWithSecondPrecision = quoteTimeWithSecondPrecision - standardTimeOffset;
		}

		long quoteTimeWithMillisecondPrecision = quoteTimeWithSecondPrecision;
		// if quote time is not the same as before or is the firt quote time, then 
		// use the quote time as is which is the time starting from 0 millisecond
		if ( quoteTimeWithSecondPrecision != lastQuoteTimestamp )
		{
			lastQuoteTimestamp = quoteTimeWithSecondPrecision;
			lastQuoteReceivedTime = aData.receivedTime;	
			lastQuoteReceivedTimeIncrements = 0;
		}
		// if the quote time is the same as the previously one, then find the difference between
		// the current quote receive time and the previous quote receive time in millisecond and
		// use that as the millisecond portion of the quote time since the quote time reported is in 1s unit
		else
		{
			lastQuoteReceivedTimeIncrements++;
			long timeDifference = Math.abs( aData.receivedTime - lastQuoteReceivedTime )  + lastQuoteReceivedTimeIncrements;
			if ( timeDifference > 999 )
			{
				timeDifference = 999;
			}
			quoteTimeWithMillisecondPrecision = quoteTimeWithSecondPrecision + timeDifference;
		}
		builder.append( quoteTimeWithMillisecondPrecision );
		String bidPriceText = dataTokenizer.nextToken();
		String askPriceText = dataTokenizer.nextToken();
		BigDecimal bidPrice = new BigDecimal( bidPriceText );
		BigDecimal askPrice = new BigDecimal( askPriceText );
		builder.append( ", \"bidPrice\": " );
		builder.append( bidPriceText );
		builder.append( ", \"askPrice\": " );
		builder.append( askPriceText );
		builder.append( ", \"spread\": " );
		builder.append( askPrice.subtract( bidPrice ).toPlainString() );
		builder.append( ", \"quoteTime\": " );
		builder.append( quoteTimeWithSecondPrecision );
		builder.append( "}" );
		return builder.toString();
	}
	
	/**
	 * login response callback
	 */
	private class LoginResponseCallback implements DataCallback
	{
		public void onReceive(String aData) 
		{
System.out.println( "Receive Login Response " + aData );
			//JsonElement jsonElement = jsonParser.parse(aData);
			//JsonObject jsonObject = jsonElement.getAsJsonObject();
			//if ( jsonObject.get( "responseType" ).getAsInt()==0 )
			//{
				smashLoggedIn = true;
				publishAll();				
			//}
		}
	}
	
	/**
	 * shutdown hook
	 */
	private class ShutdownHook implements Runnable
	{
		public void run() 
		{
			dispose();
		}
	}
	
	/**
	 * event listener
	 */
	private class MetaTraderEventListener implements DDEClientEventListener
	{
		private CountDownLatch eventDisconnect;
		
		/**
		 * create MetaTraderEventListener
		 * @param anEventDisconnect event disconnect handling
		 */
		private	MetaTraderEventListener( CountDownLatch anEventDisconnect )
		{
			eventDisconnect = anEventDisconnect;
		}
		
		public void onDisconnect() 
		{
			System.out.println( "Disconnected" );
			eventDisconnect.countDown();
		}

		public void onItemChanged(String aTopic, String anItem, String aData) 
		{
			QueuedData data = new QueuedData( System.currentTimeMillis(), anItem, aData );
			queue.offer( data );
		}
	}
	
	private class QueuedData
	{
		private long receivedTime;
		private String item;
		private String data;
		
		private QueuedData( long aReceivedTime, String anItem, String aData )
		{
			receivedTime = aReceivedTime;
			item = anItem;
			data = aData;
		}
	}
	
	private class PublishToSmashTask implements Runnable
	{
		public void run()
		{
			while( !disposed )
			{
				try
				{
					QueuedData data = queue.poll( 10, TimeUnit.SECONDS );
					if ( data != null )
					{
						String datasetUUID = symbolToDatasetUUIDLookup.get( data.item );
						if ( datasetUUID != null )
						{						
							String jsonData = toJSON(datasetUUID, data);
System.out.println( "POST DATA " + datasetUUID + " " + jsonData + " Size " + queue.size() );	
							client.publishData(datasetUUID, jsonData);
						}
					}
				}
				catch( InterruptedException e )
				{
					
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
		private String datasetUUID;
		private String symbol;
		
		/**
		 * create PublishRequestResponseCallback
		 * @param aDatasetUUID data set UUID
		 * @param aSymbol symbol
		 */
		private PublishRequestResponseCallback( String aDatasetUUID, String aSymbol ){
			datasetUUID = aDatasetUUID;
			symbol = aSymbol;
		}
		
		public void onReceive(String aData) 
		{
System.out.println( "Receive Publish Response " + aData );
			JsonElement jsonElement = jsonParser.parse(aData);
			JsonObject jsonObject = jsonElement.getAsJsonObject();
			if ( "SUCCESS".equals( jsonObject.get( "status" ).getAsString() ) )
			{
				if ( canPerformDDE )
				{
					try{
System.out.println( "Subscribe to " + symbol );						
						conversation.startAdvice(symbol);
					}
					catch( Throwable t ){
						t.printStackTrace();
					}
				}
			}
		}
	}
	
    public static void main( String[] args )
    {
    	try
    	{
    		Properties properties = new Properties();
    		properties.load( new FileInputStream(args[0]));
    		SmashMetaTraderPublisher publisher = new SmashMetaTraderPublisher( 
    												properties.getProperty( "smash.metaTraderPublisher.smashHost" ),
    												properties.getProperty( "smash.metaTraderPublisher.smashVPN" ),
    												properties.getProperty( "smash.metaTraderPublisher.smashUserId" ),
    												properties.getProperty( "smash.metaTraderPublisher.smashPassword" ),
    												properties.getProperty( "smash.metaTraderPublisher.smashMetaTraderQuoteTypeId" ));
    		StringTokenizer subscriptionMappingConfig = new StringTokenizer( 
    											properties.getProperty( "smash.metaTraderPublisher.subscriptionMapping" ), "," );
    		daylightSavingTimeOffset = Long.parseLong( properties.getProperty( "smash.metaTraderPublisher.daylightSavingTimeOffset") );
    		standardTimeOffset = Long.parseLong( properties.getProperty( "smash.metaTraderPublisher.standardTimeOffset") );
    		
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
    		publisher.initialize();
    		publisher.await();
    	}
    	catch( Throwable t )
    	{
    		t.printStackTrace();
    	}
    }
}
