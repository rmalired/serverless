package indiv.rakesh.serverless.loadcsv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.collect.Lists;

import au.com.bytecode.opencsv.CSVReader;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

    public LambdaFunctionHandler() {}
    
    /** Provide the AWS region which your DynamoDB table is hosted. */
	Region AWS_REGION = Region.getRegion(Regions.US_EAST_1);

	/** The DynamoDB table name. */
	String DYNAMO_TABLE_NAME = "StockPrice";

    // Test purpose only.
    LambdaFunctionHandler(AmazonS3 s3) {
        this.s3 = s3;
    }

    @Override
    public String handleRequest(S3Event event, Context context) {
        context.getLogger().log("Received event: " + event);
        
        Helper helper = new Helper();

        // Get the object from the event and show its content type
        String bucket = event.getRecords().get(0).getS3().getBucket().getName();
        String key = event.getRecords().get(0).getS3().getObject().getKey();      
        try {
            S3Object response = s3.getObject(new GetObjectRequest(bucket, key));
            String stockTick = key.substring(0,key.lastIndexOf("."));
            context.getLogger().log("Stock tick" + stockTick);
        
            InputStream is = response.getObjectContent();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            CSVReader reader = new CSVReader(br,',','\'',1);
            
            AmazonDynamoDB dynamoDBClient = new AmazonDynamoDBClient();
			dynamoDBClient.setRegion(AWS_REGION);
			DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
            
            TableWriteItems tickItems = new TableWriteItems(DYNAMO_TABLE_NAME);
            
            List<Item> itemList = new ArrayList<Item>();
            
            String [] lineData;
            
            while((lineData = reader.readNext())!= null){
            	Item item = helper.parseIt(lineData, stockTick);
            	itemList.add(item);
            }
            
            context.getLogger().log("Mapped all the records : size is -->"+ itemList.size());            
            for(List<Item> partition: Lists.partition(itemList, 25)){            	
            	tickItems.withItemsToPut(partition);
            	BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(tickItems);
            	
            	do{
            		Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

					if (outcome.getUnprocessedItems().size() > 0) {
						context.getLogger().log("Retrieving the unprocessed " + String.valueOf(outcome.getUnprocessedItems().size())
								+ " items.");
						outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
					}
            	}while(outcome.getUnprocessedItems().size()>0);
            }
            
            reader.close();
            br.close();
            isr.close();
            response.close();
            
        }catch(ParseException pe){
        	pe.printStackTrace();
        }catch(IOException ioe){
        	ioe.printStackTrace();
        }catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
                "Error getting object %s from bucket %s. Make sure they exist and"
                + " your bucket is in the same region as this function.", bucket, key));
            throw e;
        }
		return "SUCCESS";
    }
}