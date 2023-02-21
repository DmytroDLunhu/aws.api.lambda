package lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import model.Product;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;

public class ProductLambdaHandler implements RequestStreamHandler {
    private String DYNAMO_TABLE = "Products";

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        JSONParser parser = new JSONParser();           // parse request object
        JSONObject responseObject = new JSONObject();   // add object to API response
        JSONObject responseBody = new JSONObject();     // add item to object

        AmazonDynamoDB client = AmazonDynamoDBAsyncClientBuilder.defaultClient();
        DynamoDB dynamoDB = new DynamoDB(client);

        int id;
        Item resItem = null;

        try {
            JSONObject reqObject = (JSONObject) parser.parse(reader);
            //pathParams
            if (reqObject.get("pathParams") != null) {
                JSONObject pps = (JSONObject) reqObject.get("pathParams");
                if (pps.get("id") != null) {
                    id = Integer.parseInt((String) pps.get("id"));
                    resItem = dynamoDB.getTable(DYNAMO_TABLE).getItem("id", id);
                }
                //queryStringParams
                else if (reqObject.get("queryStringParams") != null) {
                    JSONObject qsp = (JSONObject) reqObject.get("queryStringParams");
                    if (qsp.get("id") != null) {
                        id = Integer.parseInt((String) qsp.get("id"));
                        resItem = dynamoDB.getTable(DYNAMO_TABLE).getItem("id", id);
                    }
                }
                if (resItem != null) {
                    Product product = new Product(resItem.toJSON());
                    responseBody.put("product", product);
                    responseObject.put("statusCode", 200);
                } else {
                    responseBody.put("message", "No item found");
                    responseObject.put("statusCode", 404);
                }
                responseObject.put("body", responseBody.toString());
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        writer.write(responseObject.toString());
        reader.close();
        writer.close();
    }
}