/**
 * 
 */
package indiv.rakesh.serverless.loadcsv;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.amazonaws.services.dynamodbv2.document.Item;

/**
 * @author rakesh.malireddy
 *
 */
public class Helper {
	
	public Item parseIt(String[] line,String tick) throws ParseException{
		
		Item item = new Item();
		String date = line[0];
		DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
		df.setTimeZone(TimeZone.getTimeZone("GMT-5:00"));
		Date parsedDate = df.parse(date);
		long dateInSecs = parsedDate.getTime()/1000;
		item.withPrimaryKey("Tick", tick, "cdate", dateInSecs);
		item.withNumber("Open", new BigDecimal(line[1]));
		item.withNumber("High", new BigDecimal(line[2]));
		item.withNumber("Low", new BigDecimal(line[3]));
		item.withNumber("Close", new BigDecimal(line[4]));
		item.withNumber("AdjClose", new BigDecimal(line[5]));
		item.withNumber("Volume", new BigDecimal(line[6]));
		
		
		return item;
	}

}
