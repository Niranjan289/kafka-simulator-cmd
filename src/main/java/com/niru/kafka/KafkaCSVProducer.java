package com.niru.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class KafkaCSVProducer {
	
	private final static String CLIENT_ID = "KafkaCSVProducer";
	private String topic;
    private String bootstrapServer;
    private int totalRecords;
    private int fieldsCount;
	private String metadata;
	private LinkedHashMap<String, String> fields = new LinkedHashMap<String, String>();
	private int delay; // delay for producing each message (milliseconds)
	
	private int radius = 100;
	private String latitude = "29.951065";
	private String longitude = "-90.071533";
    
     
    
    public KafkaCSVProducer() {
    	Properties prop = new Properties();
    	InputStream input = null;

    	try {

    		ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    		input = classloader.getResourceAsStream("kafka_csv.properties");

    		// load a properties file
    		prop.load(input);

    		// get the property value and print it out
    		bootstrapServer = prop.getProperty("kafka_server");
    		totalRecords = Integer.parseInt(prop.getProperty("no_of_records"));
    		fieldsCount = Integer.parseInt(prop.getProperty("no_of_fields"));
    		delay=  Integer.parseInt(prop.getProperty("delay"));
    		topic = prop.getProperty("topic");
    		metadata = prop.getProperty("metadata");
    		
    		metadata = metadata.replace("{", "");
    		metadata = metadata.replace("}", "");
    		metadata = metadata.replace("\"","");

    		StringTokenizer st = new StringTokenizer(metadata,",");
    		while (st.hasMoreTokens()) {  
    	         String nextToken = st.nextToken();
    	         String[] split = nextToken.split(":");
    	         if(split != null && split.length == 2) {
    	        	 fields.put(split[0], split[1]);
    	         }
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
    	String csvMessage = "";
    	
    	for(Iterator iterator = fields.keySet().iterator(); iterator.hasNext();) {
    	    String key = (String) iterator.next();
    	    String value = (String) fields.get(key);
    	    Random rand = new Random();
		  
		    if(value.equalsIgnoreCase("Integer")) {
		    	int min = 1;
		    	int max = 150000;
		    	int randomNum = rand.nextInt((max - min) + 1) + min;
		        csvMessage = csvMessage + i + ",";
		    } else if(value.equalsIgnoreCase("String")) {
		    if(i % 2 ==0){
        csvMessage = csvMessage+"High"+ ",";
        }
        else if(i % 3 ==0){
        
        csvMessage  =  csvMessage+"Medium"+ ",";
        } else if(i % 4 ==0){
       csvMessage  =  csvMessage+"Critical"+ ",";
        }
        else{
        
        csvMessage  =  csvMessage+"Low"+ ",";
        }
		    
		    	
		    } 
		    else if(value.equalsIgnoreCase("Date")) {
		    	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		    	csvMessage = csvMessage +sdf.format(new Date()) + ",";
		    } else if(value.equalsIgnoreCase("Long")) {
		    	long randomNum = rand.nextLong();
		        csvMessage = csvMessage + randomNum + ",";
		    } else if(value.equalsIgnoreCase("Float")) {
		    	float randomNum = rand.nextFloat();
		        csvMessage = csvMessage + randomNum + ",";
		    } else if(value.equalsIgnoreCase("Double")) {
		    	double randomNum = rand.nextDouble();
		        csvMessage = csvMessage + randomNum + ",";
		    } else if(value.equalsIgnoreCase("Boolean")) {
		    	boolean randomNum = rand.nextBoolean();
		        csvMessage = csvMessage + randomNum + ",";
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
				csvMessage = csvMessage + latitude + ",";
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
				csvMessage = csvMessage + longitude + ",";
		    }
    	}
		return StringUtils.stripEnd(csvMessage, ",");
    }
    
    
    public void runProducer() {
    	final Producer<Long, String> producer = createProducer();
    	//for (int i = 0; i < totalRecords; i++) {
    	int i =0;
    	while(true) {
    		String message = null;
				if(fieldsCount != fields.size()) {
					System.out.println("no_of_fields should be equal to metadata array size");
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
			i++;
		}
		producer.close();
    }
    
    public static void main(String... args) throws Exception {
    	KafkaCSVProducer basicProducer =  new KafkaCSVProducer();
    	basicProducer.runProducer();
    }
}
