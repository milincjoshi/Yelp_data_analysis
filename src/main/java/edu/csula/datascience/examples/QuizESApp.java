package edu.csula.datascience.examples;

import com.google.gson.Gson;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.aggregations.support.format.ValueFormat.DateTime;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Quiz elastic search app to see Salaries.csv file better
 *
 * gradle command to run this app `gradle esQuiz`
 *
 * Before you send data, please run the following to update mapping first:
 *
 * ```
 PUT /yelp-data
 {
     "mappings" : {
         "users" : {
             "properties" : {
                 "yelping_since" : {
                     "type" : "string"
                 },
                 "review_count" : {
                     "type" : "integer"
                 },
                 "name" : {
                     "type" : "string"
                 },
                 "user_id" : {
                     "type" : "string"
                 },
                 "average_stars" : {
                     "type" : "float"
                 }
             }
         }
     }
 }
 ```
 */
public class QuizESApp {
    private final static String indexName = "yelp-data";
    private final static String typeName = "users";
    
    private final static String business_indexname = "yelp_business";
    private final static String business_typename = "business";

    private final static String tip_indexname = "yelp_tip";
    private final static String tip_typename = "tip";

    private final static String users_indexname = "yelp_users";
    private final static String users_typeName = "users";

    public static void main(String[] args) throws URISyntaxException, IOException, ParseException {

    	insert_business();
    	insert_users();
    	insert_tip();
        
    }
    
    public static void insert_business()throws URISyntaxException, IOException, ParseException{
    	Node node = nodeBuilder().settings(Settings.builder()
                .put("vallabh", "your-namex`")
                .put("path.home", "elasticsearch-data")).node();
            Client client = node.client();


    //insert user
            File csv = new File(
                ClassLoader.getSystemResource("yelp_academic_dataset_business.json")
                    .toURI()
            );

            BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        System.out.println("Facing error while importing data to elastic search");
                        failure.printStackTrace();
                    }
                })
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(
                    BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
            // Gson library for sending json to elastic search
            
            Gson gson = new Gson();

            try {
                // after reading the csv file, we will use CSVParser to parse through
                // the csv files
                /*
                CSVParser parser = CSVParser.parse(
                    csv,
                    Charset.defaultCharset(),
                    CSVFormat.EXCEL.withHeader()
                );
                */
                //Parsing
                JSONParser parser = new JSONParser();
                String filePath = "C:\\Users\\vallabh\\git\\datascience-spring-2016\\src\\main\\resources\\yelp_academic_dataset_business.json";
//              Object obj = parser.parse(new FileReader("C:\\Users\\vallabh\\git\\datascience-spring-2016\\src\\main\\resources\\yelp_academic_dataset_user.json"));
//              JSONObject jsonObject = (JSONObject) obj;
//              String name = (String) jsonObject.get("name");
//              System.out.println(name);
                //End Parsing

                // for each record, we will insert data into Elastic Search
                //Parse JSON
                FileInputStream fis = new FileInputStream(filePath);
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));      
                String line = null;
                int i = 0;
                while ((line = br.readLine()) != null) {

                 	System.out.println(i);
                    i++;
                	System.out.println(line);

                	//retrieving business object details
                    Object business_obj = parser.parse(line);
                    JSONObject business_jsonObject = (JSONObject) business_obj;
                    String business_id = (String) business_jsonObject.get("business_id");
                    Boolean open = (Boolean)business_jsonObject.get("open");
                    String city = (String)business_jsonObject.get("city");
                    long review_count = (long)business_jsonObject.get("review_count");
                    String business_name = (String)business_jsonObject.get("name");
                    double longitude = (double)business_jsonObject.get("longitude");
                    double latitude = (double)business_jsonObject.get("latitude");
                    String state =(String)business_jsonObject.get("state");
                    double stars = (double)business_jsonObject.get("stars");
                    String type = (String)business_jsonObject.get("type");
                    
                    //assigning business object details
                    Business business = new Business();
                    business.business_id = business_id;
                    business.open = open;
                    business.city = city;
                    business.review_count = review_count;
                    business.business_name = business_name;
                    business.longitude = longitude;
                    business.latitude = latitude;
                    business.state = state;
                    business.stars = stars;
                    business.type = type;
                    
                    bulkProcessor.add(new IndexRequest(business_indexname, business_typename)
                        .source(gson.toJson(business))
                    );
                }
                br.close();
                //End Parse JSON

                /*
                parser.forEach(record -> {

                    String name = record.get("name");
                    int reviews_count = Integer.parseInt(record.get("review_count"));
                    String yelping_since = record.get("yelping_since");
                    String user_id = record.get("user_id");
                    float average_stars = Float.parseFloat(record.get("average_stars"));
                    
                    User user = new User();
                    user.name = name;
                    user.reviews_count = reviews_count;
                    user.average_rating = average_stars;
                    user.user_id = user_id;
                    user.yelping_since = yelping_since;
                    bulkProcessor.add(new IndexRequest(indexName, typeName)
                        .source(gson.toJson(user))
                    );
                });
                */
            } catch (IOException e) {
                e.printStackTrace();
            }
    }
    public static void insert_tip()throws URISyntaxException, IOException, ParseException{
    	Node node = nodeBuilder().settings(Settings.builder()
                .put("vallabh", "your-namex`")
                .put("path.home", "elasticsearch-data")).node();
            Client client = node.client();


    //insert user
            File csv = new File(
                ClassLoader.getSystemResource("yelp_academic_dataset_tip.json")
                    .toURI()
            );

            BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        System.out.println("Facing error while importing data to elastic search");
                        failure.printStackTrace();
                    }
                })
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(
                    BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
            // Gson library for sending json to elastic search
            
            Gson gson = new Gson();

            try {
                // after reading the csv file, we will use CSVParser to parse through
                // the csv files
                /*
                CSVParser parser = CSVParser.parse(
                    csv,
                    Charset.defaultCharset(),
                    CSVFormat.EXCEL.withHeader()
                );
                */
                //Parsing
                JSONParser parser = new JSONParser();
                String filePath = "C:\\Users\\vallabh\\git\\datascience-spring-2016\\src\\main\\resources\\yelp_academic_dataset_tip.json";
//              Object obj = parser.parse(new FileReader("C:\\Users\\vallabh\\git\\datascience-spring-2016\\src\\main\\resources\\yelp_academic_dataset_user.json"));
//              JSONObject jsonObject = (JSONObject) obj;
//              String name = (String) jsonObject.get("name");
//              System.out.println(name);
                //End Parsing

                // for each record, we will insert data into Elastic Search
                //Parse JSON
                FileInputStream fis = new FileInputStream(filePath);
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));      
                String line = null;
                int i = 0;
                while ((line = br.readLine()) != null) {
                    
                	System.out.println(i);
                    i++;
                	System.out.println(line);

                	//retreiving tip data
                    Object tip_obj = parser.parse(line);
                    JSONObject tip_jsonObject = (JSONObject) tip_obj;
                    String tip_user_id = (String) tip_jsonObject.get("user_id");
                    String tip_business_id = (String) tip_jsonObject.get("business_id");
                    long likes = (long) tip_jsonObject.get("likes");
                    String date = (String) tip_jsonObject.get("date");
                    String tip_type = (String) tip_jsonObject.get("type");
                    
                    //assigning tip data
                    Tip tip  = new Tip();
                    tip.business_id = tip_business_id;
                    tip.likes = likes;
                    tip.user_id = tip_user_id;
                    tip.date = date;
                    tip.type = tip_type;
                    
                    bulkProcessor.add(new IndexRequest(tip_indexname, tip_typename)
                        .source(gson.toJson(tip))
                    );
                }
                br.close();
                //End Parse JSON

                /*
                parser.forEach(record -> {

                    String name = record.get("name");
                    int reviews_count = Integer.parseInt(record.get("review_count"));
                    String yelping_since = record.get("yelping_since");
                    String user_id = record.get("user_id");
                    float average_stars = Float.parseFloat(record.get("average_stars"));
                    
                    User user = new User();
                    user.name = name;
                    user.reviews_count = reviews_count;
                    user.average_rating = average_stars;
                    user.user_id = user_id;
                    user.yelping_since = yelping_since;
                    bulkProcessor.add(new IndexRequest(indexName, typeName)
                        .source(gson.toJson(user))
                    );
                });
                */
            } catch (IOException e) {
                e.printStackTrace();
            }
    }
    public static void insert_users()throws URISyntaxException, IOException, ParseException{
    	Node node = nodeBuilder().settings(Settings.builder()
                .put("vallabh", "your-namex`")
                .put("path.home", "elasticsearch-data")).node();
            Client client = node.client();


    //insert user
            File csv = new File(
                ClassLoader.getSystemResource("yelp_academic_dataset_user.json")
                    .toURI()
            );

            BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        System.out.println("Facing error while importing data to elastic search");
                        failure.printStackTrace();
                    }
                })
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(
                    BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
            // Gson library for sending json to elastic search
            
            Gson gson = new Gson();

            try {
                // after reading the csv file, we will use CSVParser to parse through
                // the csv files
                /*
            	CSVParser parser = CSVParser.parse(
                    csv,
                    Charset.defaultCharset(),
                    CSVFormat.EXCEL.withHeader()
                );
                */
                //Parsing
                JSONParser parser = new JSONParser();
                String filePath = "C:\\Users\\vallabh\\git\\datascience-spring-2016\\src\\main\\resources\\yelp_academic_dataset_user.json";
//        		Object obj = parser.parse(new FileReader("C:\\Users\\vallabh\\git\\datascience-spring-2016\\src\\main\\resources\\yelp_academic_dataset_user.json"));
//        		JSONObject jsonObject = (JSONObject) obj;
//        		String name = (String) jsonObject.get("name");
//        		System.out.println(name);
        		//End Parsing

                // for each record, we will insert data into Elastic Search
        		//Parse JSON
        		FileInputStream fis = new FileInputStream(filePath);
         		BufferedReader br = new BufferedReader(new InputStreamReader(fis));    	 
        		String line = null;
        		int i = 0;
        		while ((line = br.readLine()) != null) {
        			
        			System.out.println(i);
                    i++;
                	System.out.println(line);

                	//Retrieving user object details
        			Object user_obj = parser.parse(line);
            		JSONObject user_jsonObject = (JSONObject) user_obj;
            		String name = (String) user_jsonObject.get("name");      			
            		long reviews_count = (long)user_jsonObject.get("review_count");
                	String yelping_since = (String) user_jsonObject.get("yelping_since");
                	String user_id = (String) user_jsonObject.get("user_id");
                	double average_stars = (double)user_jsonObject.get("average_stars");
         
                	//assignning user object details
                	User user = new User();
                	user.name = name;
                	user.review_count = reviews_count;
                	user.average_stars = average_stars;
                	user.user_id = user_id;
                	user.yelping_since = yelping_since;
               
                	bulkProcessor.add(new IndexRequest(users_indexname, users_typeName)
                        .source(gson.toJson(user))
                    );
        		}
        		br.close();
        		//End Parse JSON

        		/*
                parser.forEach(record -> {

                	String name = record.get("name");
                	int reviews_count = Integer.parseInt(record.get("review_count"));
                	String yelping_since = record.get("yelping_since");
                	String user_id = record.get("user_id");
                	float average_stars = Float.parseFloat(record.get("average_stars"));
                	
                	User user = new User();
                	user.name = name;
                	user.reviews_count = reviews_count;
                	user.average_rating = average_stars;
                	user.user_id = user_id;
                	user.yelping_since = yelping_since;
                    bulkProcessor.add(new IndexRequest(indexName, typeName)
                        .source(gson.toJson(user))
                    );
                });
                */
            } catch (IOException e) {
                e.printStackTrace();
            }

    }

    private static Double parseSafe(String value) {
        return Double.parseDouble(value.isEmpty() || value.equals("Not Provided") ? "0" : value);
    }

    static class Business{
	//Single JSON Object
	//{"business_id": "5UmKMjUEUNdYWqANhGckJw", "full_address": "4734 Lebanon Church Rd\nDravosburg, PA 15034", "hours": {"Friday": {"close": "21:00", "open": "11:00"}, "Tuesday": {"close": "21:00", "open": "11:00"}, "Thursday": {"close": "21:00", "open": "11:00"}, "Wednesday": {"close": "21:00", "open": "11:00"}, "Monday": {"close": "21:00", "open": "11:00"}}, "open": true, "categories": ["Fast Food", "Restaurants"], "city": "Dravosburg", "review_count": 4, "name": "Mr Hoagie", "neighborhoods": [], "longitude": -79.9007057, "state": "PA", "stars": 4.5, "latitude": 40.3543266, "attributes": {"Take-out": true, "Drive-Thru": false, "Good For": {"dessert": false, "latenight": false, "lunch": false, "dinner": false, "brunch": false, "breakfast": false}, "Caters": false, "Noise Level": "average", "Takes Reservations": false, "Delivery": false, "Ambience": {"romantic": false, "intimate": false, "classy": false, "hipster": false, "divey": false, "touristy": false, "trendy": false, "upscale": false, "casual": false}, "Parking": {"garage": false, "street": false, "validated": false, "lot": false, "valet": false}, "Has TV": false, "Outdoor Seating": false, "Attire": "casual", "Alcohol": "none", "Waiter Service": false, "Accepts Credit Cards": true, "Good for Kids": true, "Good For Groups": true, "Price Range": 1}, "type": "business"}
	    String business_id;
	    String address;
	    Boolean open;
		String city;
		long review_count;
		String business_name;
		double longitude;
		String state;
		double stars;
		double latitude;
		String type;	
		
	    ArrayList<String> categories;
		ArrayList<String> neighborhoods;

    }
    static class Tip{
	//Single JSON Object
	//{"user_id": "-6rEfobYjMxpUWLNxszaxQ", "text": "Don't waste your time.", "business_id": "cE27W9VPgO88Qxe4ol6y_g", "likes": 0, "date": "2013-04-18", "type": "tip"}
    	String user_id;
    	String business_id;
    	long likes;
    	String date;
    	String type;
    }
    static class User{
	//Single JSON Object
	//{"yelping_since": "2011-09", "votes": {"funny": 3, "useful": 1, "cool": 3}, "review_count": 3, "name": "Tulsi", "user_id": "yWno-feYB6VW-3fMsn-3dw", "friends": [], "fans": 0, "average_stars": 4.0, "type": "user", "compliments": {}, "elite": []}
    	String yelping_since;
        long review_count; 
    	String name;
    	String user_id;
    	int fans;
    	double average_stars;
    	String type;
    }
}

