package com.trafficmove.app.kafka.producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import com.trafficmove.avro.SensorData;
//import com.trafficmove.app.kafka.vo.SensorData;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class SensorDataProducer {
	
	private static final Logger logger = Logger.getLogger(SensorDataProducer.class);

	public static void main(String[] args) throws Exception {
		//read config file		
		
		Config appConfig = ConfigFactory.defaultApplication().resolve();		
		String bootstrap = appConfig.getString("bootstrapservers");
		String topic = appConfig.getString("topicname");
		String schemaUrl = appConfig.getString("schemaregistryurl");
		logger.info("Using bootstrap-list=" + bootstrap + " and topic " + topic);

		// set producer properties
		Properties properties = new Properties();
		properties.put("bootstrap.servers", bootstrap);
		properties.put("request.required.acks", "1");
		//properties.put("serializer.class", "com.iot.app.kafka.util.IoTDataEncoder");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		properties.put("schema.registry.url", schemaUrl);
		
		System.out.println("Starting Stream Producer...");
		
		//generate event
		KafkaProducer<String, SensorData> producer = new KafkaProducer<String, SensorData>(properties);
		SensorDataProducer sensorProducer = new SensorDataProducer();
		sensorProducer.generateSensorEvent(producer,topic);		
	}


	/**
	 * Method runs in while loop and generates random IoT data in JSON with below format. 
	 * 
	 * {"vehicleId":"52f08f03-cd14-411a-8aef-ba87c9a99997","vehicleType":"Public Transport","routeId":"route-43","latitude":",-85.583435","longitude":"38.892395","timestamp":1465471124373,"speed":80.0,"fuelLevel":28.0}
	 * 
	 * @throws InterruptedException 
	 * 
	 * 
	 */
	private void generateSensorEvent(KafkaProducer<String, SensorData> producer, String topic) throws InterruptedException {
		List<String> routeList = Arrays.asList(new String[]{"Route-1", "Route-2", "Route-3"});//, "Route-4", "Route-5"});
		List<String> vehicleTypeList = Arrays.asList(new String[]{"Large Truck", "Small Truck", "Private Car", "Bus", "Taxi"});
		Random rand = new Random();
		logger.info("Sending events");
		// generate event in loop, 
		while (true) {
			List<SensorData> eventList = new ArrayList<SensorData>();
			for (int i = 0; i < 100; i++) {// create 100 vehicles
				String vehicleId = UUID.randomUUID().toString();
				String vehicleType = vehicleTypeList.get(rand.nextInt(5));
				String routeId = routeList.get(rand.nextInt(3));
				long timestamp = new Date().getTime();
				double speed = rand.nextInt(100 - 20) + 20;// random speed between 20 to 100
				double fuelLevel = rand.nextInt(40 - 10) + 10;
				for (int j = 0; j < 5; j++) {// Add 5 events for each vehicle
					String coords = getCoordinates(routeId);
					String latitude = coords.substring(0, coords.indexOf(","));
					String longitude = coords.substring(coords.indexOf(",") + 1, coords.length());
					SensorData event = new SensorData(vehicleId, vehicleType, routeId, latitude, longitude, timestamp, speed,fuelLevel);
					eventList.add(event);
				}
			}
			Collections.shuffle(eventList);// shuffle for random events
			for (SensorData event : eventList) {
				ProducerRecord<String, SensorData> data = new ProducerRecord<String, SensorData>(topic, (String) event.getVehicleId(), event);
				producer.send(data);
				System.out.println("record sent: "+ event );
				Thread.sleep(rand.nextInt(3000 - 1000) + 1000);//random delay of 1 to 3 seconds
			}
		}
	}
	
	//Method to generate random latitude and longitude for routes
	private String  getCoordinates(String routeId) {
		Random rand = new Random();
		int latPrefix = 0;
		int longPrefix = -0;
		if (routeId.equals("Route-1")) {
			latPrefix = 33;
			longPrefix = -96;
		} else if (routeId.equals("Route-2")) {
			latPrefix = 34;
			longPrefix = -97;
		} else if (routeId.equals("Route-3")) {
			latPrefix = 35;
			longPrefix = -98;
		} else if (routeId.equals("Route-4")) {
			latPrefix = 36;
			longPrefix = -99;
		} else if (routeId.equals("Route-5")) {
			latPrefix = 37;
			longPrefix = -100;
		} 
		Float lati = latPrefix + rand.nextFloat();
		Float longi = longPrefix + rand.nextFloat();
		return lati + "," + longi;
	}
}
