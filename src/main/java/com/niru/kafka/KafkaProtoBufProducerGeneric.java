package com.niru.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;

import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

public class KafkaProtoBufProducerGeneric {
	
	private final static String CLIENT_ID = "KafkaProtoBufProducerGeneric";
	private String topic;
    private String bootstrapServer;
    private int totalRecords;
	private int delay ; // delay for producing each message (milliseconds)
    private Class loadClass;
    String messageName;
     
    
    public KafkaProtoBufProducerGeneric() {
    	Properties prop = new Properties();
    	InputStream input = null;
    	
    	
    	

    	try {

    		ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    		input = classloader.getResourceAsStream("protoBuff.properties");
    		prop.load(input);

    		bootstrapServer = prop.getProperty("kafka_server");
    		totalRecords = Integer.parseInt(prop.getProperty("no_of_records"));
    		topic = prop.getProperty("topic");
    		delay= Integer.parseInt(prop.getProperty("delay"));
    		loadClass = classloader.loadClass(prop.getProperty("className"));
    		
    	} catch (IOException | ClassNotFoundException ex) {
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
    
    private Producer<Long, byte[]> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }
    
    @SuppressWarnings("unchecked")
	private byte[] generateMessage(int i) {
    	
    	
    	ByteArrayOutputStream byteOut =null;
    	List<Descriptor> messageObject =null;
    	Map<String, Object> fieldList = new LinkedHashMap<String, Object>();
    	try {
			i++;
			Method method = loadClass.getMethod("getDescriptor");
			com.google.protobuf.Descriptors.FileDescriptor invoke = (com.google.protobuf.Descriptors.FileDescriptor) method.invoke(com.google.protobuf.Descriptors.FileDescriptor.class);
			messageObject =invoke.getMessageTypes();//getDefaultInstanceForType();
			String messagePath = "";
			for(Descriptor des:messageObject){
				getFieldDescriptors(des, messagePath, fieldList);
			}
			 byteOut = new ByteArrayOutputStream();
		    ObjectOutputStream out = new ObjectOutputStream(byteOut);
		    out.writeObject(fieldList);
				
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | IOException e) {
			e.printStackTrace();
		}
    	
		return byteOut.toByteArray();
    }
    
    public Map<String, Object> getFieldDescriptors(Descriptor des, String messagePath, Map<String, Object> fieldList) {
		
		 Random rand = new Random();
			
			for (FieldDescriptor indp : des.getFields()) {
				String currentPath ="";
				if(messagePath.isEmpty())
					currentPath=messagePath;
				else
					currentPath=messagePath+"$";
				if (indp.getType() == com.google.protobuf.Descriptors.FieldDescriptor.Type.MESSAGE) {
						getFieldDescriptors(indp.getMessageType(), currentPath +des.getName(),fieldList);
				} else if(indp.getType() == com.google.protobuf.Descriptors.FieldDescriptor.Type.STRING){
					int min = 1;
			    	int max = 999;
			    	int randomNum = rand.nextInt((max - min) + 1) + min;
					fieldList.put(currentPath+des.getName()+"$"+ indp.getName(), messagePath+ "$" +des.getName()+"$"+ indp.getName()+"Value"+randomNum);
				}else if(indp.getType() == com.google.protobuf.Descriptors.FieldDescriptor.Type.BYTES){
					int min = 1;
			    	int max = 9;
			    	int randomNum = rand.nextInt((max - min) + 1) + min;
					fieldList.put(currentPath+des.getName()+"$"+ indp.getName(), randomNum);
				}else{
					int min = 1;
			    	int max = 999;
					int randomNum = rand.nextInt((max - min) + 1) + min;
					fieldList.put(currentPath+des.getName()+"$"+ indp.getName(), randomNum);
				}
			}

		return fieldList;
	}
    
    
    
    public void runProducer() {

    	final Producer<Long, byte[]> producer = createProducer();

    	for (int i = 0; i < totalRecords; i++) {
    		final ProducerRecord<Long, byte[]> record = new ProducerRecord<>(topic, 0L ,generateMessage(i));
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
    	KafkaProtoBufProducerGeneric basicProducer =  new KafkaProtoBufProducerGeneric();
    	basicProducer.runProducer();
    }
}
    
    
