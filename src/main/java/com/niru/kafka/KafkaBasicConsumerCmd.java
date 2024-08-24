package com.niru.kafka;

import java.io.File;
import java.io.IOException;

public class KafkaBasicConsumerCmd {
	private static final String KAKFA_BASE_FOLDER = "I:\\kafka";
	//private static final String KAKFA_LOGS_FOLDER = "C:\\kafka_2.12-1.0.0\\Kafka_Logs";
	//private static final String ZOOKEEPER_LOGS_FOLDER = "C:\\kafka_2.12-1.0.0\\Zookeeper_Logs";
	private static final String KAFKA_SERVER = "localhost:9092";
	private static final String ZOOKEEPER_SERVER = "localhost:2181";
	private static final String TOPIC_NAME = "twitter-topic";
	
	
	private static String ZOOKEEPER_SERVER_START_CMD;
	private static String KAFKA_SERVER_START_CMD;
	private static String TOPIC_CREATION_CMD;
	private static String PRODUCER_CMD;
	private static String CONSUMER_CMD;
	
	
	public static void main(String[] args) {
		
		 try
		    {
	
				//deleteFolder(KAKFA_LOGS_FOLDER);
				 
				//deleteFolder(ZOOKEEPER_LOGS_FOLDER);
				 
	        	File file = new File(KAKFA_BASE_FOLDER);
	            Runtime rt = Runtime.getRuntime();
	            
	            ZOOKEEPER_SERVER_START_CMD = "cmd.exe /c start cmd.exe /k \".\\bin\\windows\\zookeeper-server-start.bat .\\config\\zookeeper.properties\"";
	            rt.exec(ZOOKEEPER_SERVER_START_CMD, null, file);
	            Thread.sleep(5000);
	            
	            KAFKA_SERVER_START_CMD = "cmd.exe /c start cmd.exe /k \".\\bin\\windows\\kafka-server-start.bat .\\config\\server.properties\" --override delete.topic.enable=true";
	            rt.exec(KAFKA_SERVER_START_CMD, null, file);
	            Thread.sleep(10000);
	            
	            TOPIC_CREATION_CMD = "cmd.exe /c start cmd.exe /k \".\\bin\\windows\\kafka-topics.bat --create --topic" + TOPIC_NAME + "--bootstrap-server localhost:9092\"";
	            rt.exec(TOPIC_CREATION_CMD, null, file);
	            Thread.sleep(5000);
	            
	          //  PRODUCER_CMD = "cmd.exe /c start cmd.exe /k \".\\bin\\windows\\kafka-console-producer.bat --broker-list "+KAFKA_SERVER+" --topic "+TOPIC_NAME+"\"";
	          //  rt.exec(PRODUCER_CMD, null, file);
	          //  Thread.sleep(5000);
	            
	            //CONSUMER_CMD = "cmd.exe /c start cmd.exe /k \".\\bin\\windows\\kafka-console-consumer.bat --bootstrap-server "+KAFKA_SERVER+" --topic "+TOPIC_NAME+"\"";
	            //rt.exec(CONSUMER_CMD, null, file);
		    }
	        catch (Exception e)
	        {
	            e.printStackTrace();
	        }

	}
	
	public static void deleteFolder(String folderPath) {
		
		File directory = new File(folderPath);

    	if(!directory.exists()){
           System.out.println("Directory does not exist.");
        }else{
           try{
               delete(directory);
           }catch(IOException e){
               e.printStackTrace();
           }
        }
	}
	
	public static void delete(File file)
	    	throws IOException{

	    	if(file.isDirectory()){

	    		//directory is empty, then delete it
	    		if(file.list().length==0){

	    		   file.delete();

	    		}else{

	    		   //list all the directory contents
	        	   String files[] = file.list();

	        	   for (String temp : files) {
	        	      //construct the file structure
	        	      File fileDelete = new File(file, temp);

	        	      //recursive delete
	        	     delete(fileDelete);
	        	   }

	        	   //check the directory again, if empty then delete it
	        	   if(file.list().length==0){
	           	     file.delete();
	        	   }
	    		}

	    	}else{
	    		//if file, then delete it
	    		file.delete();
	    	}
	    }

}
