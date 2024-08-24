package com.niru.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class KafkaJsonProducer {
	
	private final static String CLIENT_ID = "KafkaCSVProducer";
	private String topic;
    private String bootstrapServer;
    private int totalRecords;
    private int fieldsCount;
	private String metadata;
	private JSONObject fields;
	private int delay; // delay for producing each message (milliseconds)
	
	private int radius = 100000;
	private String latitude = "57.773615";
	private String longitude = "-101.687050";
    
     
    
    public KafkaJsonProducer() {
    	Properties prop = new Properties();
    	InputStream input = null;

    	try {

    		ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    		input = classloader.getResourceAsStream("kafka_json.properties");

    		// load a properties file
    		prop.load(input);

    		// get the property value and print it out
    		bootstrapServer = prop.getProperty("kafka_server");
    		totalRecords = Integer.parseInt(prop.getProperty("no_of_records"));
    		fieldsCount = Integer.parseInt(prop.getProperty("no_of_fields"));
    		delay=  Integer.parseInt(prop.getProperty("delay"));
    		topic = prop.getProperty("topic");
    		metadata = prop.getProperty("metadata");
    		
    		JSONParser jsonParser =  new JSONParser();
    		try {
    			fields = (JSONObject) jsonParser.parse(metadata);
				
			} catch (ParseException e) {
				e.printStackTrace();
			}

    	} catch (IOException ex) {
    		ex.printStackTrace();
    	} finally {
    		if (input != null) {
    			try {
    				input.close();
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
    		}
    	}
    }
    
    private Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
    
    private String generateMessage(int i) {
    	JSONObject jsonObject = new JSONObject();
    	for(Iterator iterator = fields.keySet().iterator(); iterator.hasNext();) {
    		String jsonMessage = "";
    		String key = (String) iterator.next();
     	    String value = (String) fields.get(key);
		    Random rand = new Random();
		    if(value.equalsIgnoreCase("Integer")) {
		    	int min = 0;
				int max = 99;
				int randomNum = rand.nextInt((max - min) + 1) + min;
		        jsonMessage = jsonMessage + randomNum;
		    } else if(value.equalsIgnoreCase("String")) {
		    	//jsonMessage = jsonMessage + key + "_" + i;
		    	if(i % 2 ==0){
		    		jsonMessage = jsonMessage+"High";
		     	   }
		     	   else if(i % 3 ==0){
		     		   
		     		  jsonMessage  =  jsonMessage+"Medium";
		     	   } else if(i % 4 ==0){
		     		  jsonMessage  =  jsonMessage+"Critical";
		     	   }
		     	   else{
		     		   
		     		  jsonMessage  =  jsonMessage+"Low";
		     	   }
		    } else if(value.equalsIgnoreCase("Date")) {
		    	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		    	jsonMessage = jsonMessage +sdf.format(new Date());
		    }else if(value.equalsIgnoreCase("Long")) {
		    	long randomNum = rand.nextLong();
		    	jsonMessage = jsonMessage + randomNum;
		    } else if(value.equalsIgnoreCase("Float")) {
		    	float randomNum = rand.nextFloat();
		    	jsonMessage = jsonMessage + randomNum;
		    } else if(value.equalsIgnoreCase("Double")) {
		    	double randomNum = rand.nextDouble();
		    	randomNum = randomNum *10;
		    	jsonMessage = jsonMessage + randomNum;
		    } else if(value.equalsIgnoreCase("Boolean")) {
		    	boolean randomNum = rand.nextBoolean();
		    	jsonMessage = jsonMessage + randomNum;
		    } else if(value.equalsIgnoreCase("Latitude")) {
		    	Coordinates center = new Coordinates(latitude,longitude);
		    	Random random = new Random();
				// Convert radius from meters to degrees
				double radiusInDegrees = radius / 111000f;

				double u = random.nextDouble();
				double v = random.nextDouble();
				double w = radiusInDegrees * Math.sqrt(u);
				double t = 2 * Math.PI * v;
				double x = w * Math.cos(t);
				double y = w * Math.sin(t);

				// Adjust the x-coordinate for the shrinking of the east-west distances
				double new_x = x / Math.cos(center.getLongitude());

				double foundLatitude = new_x + center.getLatitude();
				double foundLongitude = y + center.getLongitude();
				Coordinates coordinates = new Coordinates(foundLatitude, foundLongitude);
				latitude = coordinates.getLatitudeAsString();
		    	jsonMessage = jsonMessage + latitude;
		    } else if(value.equalsIgnoreCase("Longitude")) {

		    	Coordinates center = new Coordinates(latitude,longitude);
		    	Random random = new Random();
				// Convert radius from meters to degrees
				double radiusInDegrees = radius / 111000f;

				double u = random.nextDouble();
				double v = random.nextDouble();
				double w = radiusInDegrees * Math.sqrt(u);
				double t = 2 * Math.PI * v;
				double x = w * Math.cos(t);
				double y = w * Math.sin(t);
				// Adjust the x-coordinate for the shrinking of the east-west distances
				double new_x = x / Math.cos(center.getLongitude());

				double foundLatitude = new_x + center.getLatitude();
				double foundLongitude = y + center.getLongitude();
				Coordinates coordinates = new Coordinates(foundLatitude, foundLongitude);
				longitude = coordinates.getLongitudeAsString();
		    	jsonMessage = jsonMessage + longitude;
		    
		    }
		    
		    jsonObject.put(key, jsonMessage);
    	}
    	return jsonObject.toJSONString();
    }
    
    
    public void runProducer() {
    	final Producer<Long, String> producer = createProducer();
    	for (int i = 0; i < totalRecords; i++) {
    		String message = null;
				if(fieldsCount != fields.size()) {
					System.out.println("no_of_fields should be equal to metadata size");
					break;
				} else {
					message = generateMessage(i);
				}
			final ProducerRecord<Long, String> record = new ProducerRecord<>(topic, 0L ,message);
			try {
				Thread.sleep(delay);
				
				producer.send(record).get();
				
				System.out.println(record.value());
			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
    	producer.close();
    }
    
    public static void main(String... args) throws Exception {
    	KafkaJsonProducer basicProducer =  new KafkaJsonProducer();
    	basicProducer.runProducer();
    }
    
    
}
