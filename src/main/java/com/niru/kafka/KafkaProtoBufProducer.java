package com.niru.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;

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
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

public class KafkaProtoBufProducer {
	
	private final static String CLIENT_ID = "KafkaProtoBufProducer";
	private String topic;
    private String bootstrapServer;
    private int totalRecords;
	private int delay ; // delay for producing each message (milliseconds)
    private Class loadClass;
     
    
    public KafkaProtoBufProducer() {
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
    	
    	
    	ProtobufEnvelope pbe=new ProtobufEnvelope();
    	
    	Message messageObject =null;
    	try {
			i++;
			Method method = loadClass.getMethod("newBuilder");
			Builder b = (Builder) method.invoke(loadClass);
			messageObject =b.build().getDefaultInstanceForType();
			Descriptor descriptorForType = messageObject.getDescriptorForType();
			List<FieldDescriptor> fields = descriptorForType.getFields();
				for(FieldDescriptor fd:fields){
					if(fd.getType()==com.google.protobuf.Descriptors.FieldDescriptor.Type.INT32)
						pbe.addField(fd.getName(), i, Type.TYPE_INT32);
					else if(fd.getType()==com.google.protobuf.Descriptors.FieldDescriptor.Type.INT64)
						pbe.addField(fd.getName(),  i, Type.TYPE_INT64);
					else if((fd.getType()==com.google.protobuf.Descriptors.FieldDescriptor.Type.STRING))
						pbe.addField(fd.getName(), fd.getName()+"_"+ i, Type.TYPE_STRING);
					else if((fd.getType()==com.google.protobuf.Descriptors.FieldDescriptor.Type.FLOAT))
						pbe.addField(fd.getName(), fd.getName()+"_"+ i, Type.TYPE_FLOAT);
					else if((fd.getType()==com.google.protobuf.Descriptors.FieldDescriptor.Type.DOUBLE))
						pbe.addField(fd.getName(), fd.getName()+"_"+ i, Type.TYPE_DOUBLE);
					else if((fd.getType()==com.google.protobuf.Descriptors.FieldDescriptor.Type.SINT32))
						pbe.addField(fd.getName(), fd.getName()+"_"+ i, Type.TYPE_SINT32);
					else if((fd.getType()==com.google.protobuf.Descriptors.FieldDescriptor.Type.SINT64))
						pbe.addField(fd.getName(), fd.getName()+"_"+ i, Type.TYPE_SINT64);
					else if((fd.getType()==com.google.protobuf.Descriptors.FieldDescriptor.Type.BOOL))
						pbe.addField(fd.getName(), fd.getName()+"_"+ i, Type.TYPE_BOOL);
						
				}
				
				messageObject = pbe.constructMessage("ProtoMessage");
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | DescriptorValidationException e) {
			e.printStackTrace();
		}
    	
		return messageObject.toByteArray();
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
    	KafkaProtoBufProducer basicProducer =  new KafkaProtoBufProducer();
    	basicProducer.runProducer();
    }
}
    
    
