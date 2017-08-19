/**
 * 
 */
package indiv.rakesh.serverless.getquote;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

/**
 * @author rakesh.malireddy
 *
 */
public class StockFetcher {

	
	public void getQuotes(String symbols){
		
		double price = 0.0;
		int volume = 0;
		double pe = 0.0;
		double eps = 0.0;
		double week52low = 0.0;
		double week52high = 0.0;
		double daylow = 0.0;
		double dayhigh = 0.0;
		double movingav50day = 0.0;
		double marketcap = 0.0;
		String name = "";
		String currency = "";
		double shortRatio = 0.0;
		double open = 0.0;
		double previousClose = 0.0;
		String exchange="";
		String symbol="";
		
		try { 
			
			System.out.println("symbols : "+ symbols);
			
			// Retrieve CSV File
			URL yahoo = new URL("http://download.finance.yahoo.com/d/quotes.csv?s="+symbols+"&f=l1vr2ejkghm3j3nc4s7poxs");
			URLConnection connection = yahoo.openConnection(); 
			InputStreamReader is = new InputStreamReader(connection.getInputStream());
			BufferedReader br = new BufferedReader(is); 
			
			List<Stock> stocks = new ArrayList<>();
		/*	
			InputStream isr = yahoo.openStream();
			String val = getStringFromInputStream(isr);*/
			
			
			// Parse CSV Into Array
			String line ="";
			while((line = br.readLine()) != null){
				//Only split on commas that aren't in quotes
				String[] stockinfo = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
				
				// Handle Our Data
				StockMapper sh = new StockMapper();
				
				price = sh.handleDouble(stockinfo[0]);
				volume = sh.handleInt(stockinfo[1]);
				pe = sh.handleDouble(stockinfo[2]);
				eps = sh.handleDouble(stockinfo[3]);
				week52low = sh.handleDouble(stockinfo[4]);
				week52high = sh.handleDouble(stockinfo[5]);
				daylow = sh.handleDouble(stockinfo[6]);
				dayhigh = sh.handleDouble(stockinfo[7]);   
				movingav50day = sh.handleDouble(stockinfo[8]);
				marketcap = sh.handleDouble(stockinfo[9]);
				name = stockinfo[10].replace("\"", "");
				currency = stockinfo[11].replace("\"", "");
				shortRatio = sh.handleDouble(stockinfo[12]);
				previousClose = sh.handleDouble(stockinfo[13]);
				open = sh.handleDouble(stockinfo[14]);
				exchange = stockinfo[15].replace("\"", "");
				symbol = stockinfo[16].replace("\"", "");
				System.out.println("current symbol "+ symbol);
				Stock stock = new Stock(symbol, price, volume, pe, eps, week52low, week52high, daylow, dayhigh, movingav50day, marketcap, name,currency, shortRatio,previousClose,open,exchange);
				
				stocks.add(stock);
			  }
			
			  AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
			    DynamoDB dynamoDB = new DynamoDB(client);
			    Table table = dynamoDB.getTable("StockPrice");
			    
				LocalDate ldate = LocalDate.now();	
				ZonedDateTime todayZdt = ldate.atStartOfDay(ZoneId.of("America/Chicago"));	
			    
			    for(Stock stock: stocks){
			    	Item item = new Item().withPrimaryKey("Tick", stock.getSymbol(), "cdate", todayZdt.toEpochSecond())
	            		    			  .withNumber("Open", stock.getOpen())
	            		    			  .withNumber("High", stock.getDayhigh())
	            		    			  .withNumber("Low", stock.getDaylow())
	            		    			  .withNumber("Close", stock.getPrice())
	            		    			  .withNumber("AdjClose", stock.getPrice())
	            		    			  .withNumber("Volume", stock.getVolume());
			    	
			    	table.putItem(item);
			    }
			
			} catch (IOException e) {
				Logger log = Logger.getLogger(StockFetcher.class.getName()); 
				log.log(Level.SEVERE, e.toString(), e);
				//return null;
			}
		
		  
		    
		    

	}
	

}
