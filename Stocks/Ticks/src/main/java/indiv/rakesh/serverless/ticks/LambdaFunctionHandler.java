package indiv.rakesh.serverless.ticks;

import java.util.Iterator;
import java.util.Set;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;


public class LambdaFunctionHandler implements RequestHandler<Object, String> {

    @Override
    public String handleRequest(Object input, Context context) {
        /*
    	 *  identify the list of ticks and send a message to sns for each tick
    	 */
    	
        context.getLogger().log("Input: " + input);
        try {
        
	        //read data from dynamodb
	        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
	        		.withRegion(Regions.US_EAST_1).build();
	        DynamoDB dynamoDB = new DynamoDB(client);
	        
	        Table table = dynamoDB.getTable("StockList");	        
	        QuerySpec querySpec = new QuerySpec()
	        		            .withKeyConditionExpression("Tick = :tickGrp")
	        		            .withValueMap(new ValueMap()
	        		            		.with(":tickGrp", "DOW30"));
	        ItemCollection<QueryOutcome> stockList = table.query(querySpec);
	        
	        Iterator<Item> itr = stockList.iterator();
	        
	        AmazonSNS snsClient = AmazonSNSClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
	        String topicARN = "arn:aws:sns:us-east-1:049261377975:StockListPipeline";
	        
	        while(itr.hasNext()){
	        	Item item = itr.next();
	        	Set<String> ticks = item.getStringSet("TICKS");
	        	for(String tick:ticks){
	        		PublishRequest publishRequest = new PublishRequest(topicARN, tick);
	                  snsClient.publish(publishRequest);
	        	}
	        }
        }
        catch (Exception e) {
            System.err.println("Unable to query the table or place message in queue:");
            System.err.println(e.getMessage());
        }
        return "SUCCESS";
    }

}
