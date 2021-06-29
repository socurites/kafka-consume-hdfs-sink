package kafka.consume.hdfs.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consume.hdfs.sink.worker.ConsumeWorker;

public class HdfsSinkApplication {
	private final static Logger log = LoggerFactory.getLogger(HdfsSinkApplication.class);
	
	private final static String TOPIC_NAME = "select-color";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";
	private final static String GROUP_ID = "color-hdfs-save-consumer-group";
	
	private final static int CONSUMER_COUNT = 3;
	
	private final static List<ConsumeWorker> workers = new ArrayList<>();
	
	public static void main(String[] args) {
		Runtime.getRuntime().addShutdownHook(new ShutdownThread());
		
		Properties configs = buildConfigs();
		
		ExecutorService executorService = Executors.newCachedThreadPool();
		for (int i = 0; i < CONSUMER_COUNT; i++) {
			workers.add(new ConsumeWorker(configs, TOPIC_NAME, i));
		}
		
		workers.forEach(executorService::execute);
	}
	
	
	private static Properties buildConfigs() {
		Properties configs = new Properties();
		configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		
		return configs;
	}
	
	static class ShutdownThread extends Thread {
		public void run() {
			log.info("Shutting down..");
			workers.forEach(ConsumeWorker::stopAndWakeup);
		}
	}
}
