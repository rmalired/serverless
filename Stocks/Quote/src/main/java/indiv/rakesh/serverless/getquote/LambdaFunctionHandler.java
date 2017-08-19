package indiv.rakesh.serverless.getquote;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;

public class LambdaFunctionHandler implements RequestHandler<SNSEvent, String> {

    @Override
    public String handleRequest(SNSEvent event, Context context) {
        
        String message = event.getRecords().get(0).getSNS().getMessage();
        context.getLogger().log("From SNS: " + message);
        
        StockFetcher sf = new StockFetcher();
        sf.getQuotes(message);
        
        return message;
    }
}
