package com.qannouf.kafkaexample;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.Scanner;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class MyConsumer {
    private static Scanner in;
    private static boolean stop = false;

    public static void main(String[] argv)throws Exception{
    	
    		System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2");
    	
        if (argv.length != 2) {
            System.err.printf("Usage: %s <topicName> <groupId>\n",
                    MyConsumer.class.getSimpleName());
            System.exit(-1);
        }
        in = new Scanner(System.in);
        String topicName = argv[0];
        String groupId = argv[1];

        ConsumerThread consumerRunnable = new ConsumerThread(topicName,groupId);
        consumerRunnable.start();
        String line = "";
        while (!line.equals("exit")) {
            line = in.next();
        }
        consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread{
        private String topicName;
        private String groupId;
        private KafkaConsumer<String,JsonNode> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId){
            this.topicName = topicName;
            this.groupId = groupId;
        }
        public void run() {
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");

            //Figure out where to start processing messages from
            kafkaConsumer = new KafkaConsumer<String, JsonNode>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));
            //Start processing messages
            try {
                while (true) {
                    ConsumerRecords<String, JsonNode> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, JsonNode> record : records){
                    	
                    		System.out.println("received");
                    		JsonNode message= record.value();
                    		
                    		sendMessageToEndPoint(message);

                    }
                        
                }
            }catch(WakeupException ex){
                System.out.println("Exception caught " + ex.getMessage());
            }finally{
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }
        private void sendMessageToEndPoint(JsonNode json) {

            String url = "";
            String action = "";
            String client_id = "";
            
            Iterator<String> fieldNames = json.fieldNames();
            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                if (fieldName.equals("url")) {
                    JsonNode fieldValue = json.get(fieldName);
                    url = fieldValue.asText();
                }
                if (fieldName.equals("action")) {
                    JsonNode fieldValue = json.get(fieldName);
                    action = fieldValue.asText();
                }
                if (fieldName.equals("client_id")) {
                    JsonNode fieldValue = json.get(fieldName);
                    client_id = fieldValue.asText();
                }
            }
            
            ((ObjectNode)json).put("response", "Your List of Employees is : Vincent, Walid, Jofrey, Antoine");
            
            sendPostRequest(url, json);
            
		}
        
        public String sendPostRequest(String url, JsonNode jsonNode){
    		CloseableHttpClient httpclient = HttpClients.createDefault();
    		HttpPost httppost = new HttpPost(url);
    		String responseString = "";
    		String jsonText = jsonNode.toString();
    		try {
    			StringEntity requestEntity = new StringEntity(
    				    jsonText,
    				    ContentType.APPLICATION_JSON);
    			httppost.setEntity(requestEntity);
    			CloseableHttpResponse response = httpclient.execute(httppost);
    			HttpEntity entity2 = response.getEntity();
    			responseString = EntityUtils.toString(entity2, "UTF-8");
    			response.close();
    			
    		} catch (IOException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
    		System.err.println("MyConsumer.sendPostRequest done");
    		return responseString;
    	}
        
		public KafkaConsumer<String,JsonNode> getKafkaConsumer(){
           return this.kafkaConsumer;
        }
        
        public String findActionName(JsonNode message) {
        	
	    		Iterator<String> fieldNames = message.fieldNames();
	        while(fieldNames.hasNext()){
	            String fieldName = fieldNames.next();
	            if(fieldName.equals("action")) {
	            		JsonNode fieldValue = message.get(fieldName);
	            		return fieldValue.asText();
	            }
	        }
	    		
	    		return "UNDEFINED";
	    }
        
       
    }
    
    
    
}