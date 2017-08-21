package indiv.rakesh.serverless.computeindicators;

import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;

import eu.verdelhan.ta4j.Tick;
import eu.verdelhan.ta4j.TimeSeries;
import eu.verdelhan.ta4j.indicators.simple.ClosePriceIndicator;
import eu.verdelhan.ta4j.indicators.statistics.SimpleLinearRegressionIndicator;
import eu.verdelhan.ta4j.indicators.trackers.EMAIndicator;
import eu.verdelhan.ta4j.indicators.trackers.MACDIndicator;
import eu.verdelhan.ta4j.indicators.trackers.SMAIndicator;
import eu.verdelhan.ta4j.indicators.trackers.SmoothedRSIIndicator;

public class LambdaFunctionHandler implements RequestHandler<SNSEvent, String> {

    @Override
    public String handleRequest(SNSEvent event, Context context) {
        context.getLogger().log("Received event: " + event);
        String message = event.getRecords().get(0).getSNS().getMessage();
        context.getLogger().log("From SNS: " + message);
        
      //get the last 200 ticks basically 320 days backwards
        //create a timeseries --> pass it to ta-4j and compute the 5,20,40,50,200 day sma
        //create 9,26 ema and update that particular record
        
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
        		.withRegion(Regions.US_EAST_1).build();
        DynamoDB dynamoDB = new DynamoDB(client);
        
        Table table = dynamoDB.getTable("StockPrice");
        
		LocalDate ldate = LocalDate.now();	
		ZonedDateTime todayZdt = ldate.atStartOfDay(ZoneId.of("America/Chicago"));		
		LocalDate _320Back = ldate.minus(Period.ofDays(320));		
		ZonedDateTime _320DaysZdt = _320Back.atStartOfDay(ZoneId.of("America/Chicago"));
        
        QuerySpec spec = new QuerySpec()
        	    .withKeyConditionExpression("Tick = :v_tick and cdate >= :v_date")        	    
        	    .withValueMap(new ValueMap()
        	        .withString(":v_tick", message)
        	        .withNumber(":v_date", _320DaysZdt.toEpochSecond()))        	      
        	    .withConsistentRead(true)
        	    .withScanIndexForward(true);

       ItemCollection<QueryOutcome> items = table.query(spec);
       
       Iterator<Item> iterator = items.iterator();
       List<Tick> ticks = new ArrayList<>();
       while (iterator.hasNext()) {
          Item dbItem = iterator.next();
          
          //map dbItem to tick
          long cdate = dbItem.getLong("cdate");
          
          //get zonedate time out the seconds
          Instant i = Instant.ofEpochSecond(cdate);
          ZonedDateTime zdt = ZonedDateTime.ofInstant(i, ZoneId.of("America/Chicago"));
          double open = dbItem.getDouble("Open");
          double high = dbItem.getDouble("High");
          double low = dbItem.getDouble("Low");
          double close = dbItem.getDouble("AdjClose");
          double volume = dbItem.getDouble("Volume");
          
          ticks.add(new Tick(zdt, open, high, low, close, volume));
          
       }
        // create series
       TimeSeries series = new TimeSeries("AAPL", ticks);
       ClosePriceIndicator closePrice = new ClosePriceIndicator(series);
       int noOfTicks = closePrice.getTimeSeries().getTickCount();
       
       //Simple Moving average short term, intermediate term, long term
       SMAIndicator fiveDaySMA = new SMAIndicator(closePrice, 5);       
       SMAIndicator twentyDaySMA = new SMAIndicator(closePrice, 20);
       SMAIndicator thirtyDaySMA = new SMAIndicator(closePrice, 30);
       SMAIndicator sixtyDaySMA = new SMAIndicator(closePrice, 60);
       SMAIndicator fiftyDaySMA = new SMAIndicator(closePrice, 50);
       SMAIndicator twohunDaySMA = new SMAIndicator(closePrice, 200);
       
       double fiveDayval =  fiveDaySMA.getValue(series.getEnd()).toDouble();
       double twentyDayval =  twentyDaySMA.getValue(series.getEnd()).toDouble();
       double thirtyDayval =  thirtyDaySMA.getValue(series.getEnd()).toDouble();
       double sixtyDayval =  sixtyDaySMA.getValue(series.getEnd()).toDouble();
       double fiftyDayval =  fiftyDaySMA.getValue(series.getEnd()).toDouble();
       double twohunDayval =  twohunDaySMA.getValue(series.getEnd()).toDouble();
       
       
       //Exponential Moving average
       EMAIndicator ninedayEMA = new EMAIndicator(closePrice, 9);
       EMAIndicator twentysixdayEMA = new EMAIndicator(closePrice, 26);
       
       double nineDayval = ninedayEMA.getValue(noOfTicks-1).toDouble();
       double twentysixDayVal = twentysixdayEMA.getValue(noOfTicks-1).toDouble();
       
       
       //RSI for 2-day and 14-day period
       SmoothedRSIIndicator smoothRsi2 = new SmoothedRSIIndicator(closePrice, 2);
       SmoothedRSIIndicator smoothRsi14 = new SmoothedRSIIndicator(closePrice, 14);
       double twoPeriodRSI = smoothRsi2.getValue(series.getEnd()).toDouble();
       double fourteenPeriodRSI = smoothRsi14.getValue(series.getEnd()).toDouble();
       
       
       //MACD indicator
       MACDIndicator macd = new MACDIndicator(closePrice, 12, 26);
       double _macd =  macd.getValue(series.getEnd()).toDouble();
       
       //Slope(Linear Regression)
       SimpleLinearRegressionIndicator slri = new SimpleLinearRegressionIndicator(closePrice, 20);
       double slope = slri.getValue(series.getEnd()).toDouble();
       
       
       
       UpdateItemSpec updateItem = new UpdateItemSpec()
    		                       .withPrimaryKey("Tick", message, "cdate", todayZdt.toEpochSecond())
    		                       .withUpdateExpression("add #na1 :val1, #na2 :val2,  #na3 :val3, #na4 :val4, #na5 :val5, #na6 :val6, #na7 :val7, #na8 :val8, #na9 :val9, #na10 :val10, #na11 :val11")
    		                       .withNameMap(new NameMap().with("#na1", "SMA_5")
    		                    		   .with("#na2", "SMA_20")
    		                    		   .with("#na3", "SMA_30")
    		                    		   .with("#na4", "SMA_60")
    		                    		   .with("#na5", "SMA_50")
    		                    		   .with("#na6", "SMA_200")
    		                    		   .with("#na7", "EMA_9")
    		                    		   .with("#na8", "EMA_26")
    		                    		   .with("#na9", "RSI_2")
    		                    		   .with("#na10", "RSI_14")
    		                    		   .with("#na11", "SLOPE_20"))
    		                       .withValueMap(new ValueMap()
    		                    		   .with(":val1", fiveDayval)
    		                    		   .with(":val2", twentyDayval)
    		                    		   .with(":val3", thirtyDayval)
    		                    		   .with(":val4", sixtyDayval)
    		                    		   .with(":val5", fiftyDayval)
    		                    		   .with(":val6", twohunDayval)
    		                    		   .with(":val7", nineDayval)
    		                    		   .with(":val8", twentysixDayVal)
    		                    		   .with(":val9", twoPeriodRSI)
    		                    		   .with(":val10", fourteenPeriodRSI)
    		                    		   .with(":val11", slope));
       
       UpdateItemOutcome outcome = table.updateItem(updateItem);
       
        return "success";
    }
}
