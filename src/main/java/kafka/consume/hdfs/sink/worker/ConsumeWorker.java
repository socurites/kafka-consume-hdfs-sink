package kafka.consume.hdfs.sink.worker;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumeWorker implements Runnable {
	private final static Logger log = LoggerFactory.getLogger(ConsumeWorker.class);
	
	private Properties configs;
	private String topic;
	private String threadName;
	private KafkaConsumer<String, String> kafkaConsumer;
	
	private final static String HDFS_NAME = "hdfs://localhost:9000";
	private final static int FLUSH_RECORD_COUNT = 10;
	
	// Key: partitionNo, Value: list<message>
	private static Map<Integer, List<String>> bufferString = new ConcurrentHashMap<>();
	
	// Key: partitionNo, Value: First offset of bufferString
	private static Map<Integer, Long> currentFileOffset = new ConcurrentHashMap<>();;

	public ConsumeWorker(Properties configs, String topic, int threadNumber) {
		log.info(String.format("Generatting %d ConsumerWorker", threadNumber));
		this.configs = configs;
		this.topic = topic;
		this.threadName = String.format("consumer-thread-%d", threadNumber);
	}
	
	@Override
	public void run() {
		Thread.currentThread().setName(threadName);
		
		kafkaConsumer = new KafkaConsumer<>(configs);
		kafkaConsumer.subscribe(Arrays.asList(topic));
		
		try {
			poll();
		} catch (WakeupException e) {
			log.warn("Wakeup consumer");
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			kafkaConsumer.close();
		}
	}
	
	public void poll() {
		while (true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
			
			for (ConsumerRecord<String, String> record : records) {
				addHdfsFileBuffer(record);
			}
			
			saveBufferToHdfsFile(kafkaConsumer.assignment());
		}
	}
	
	private void addHdfsFileBuffer(ConsumerRecord<String, String> record) {
		List<String> buffer = bufferString.getOrDefault(record.partition(), new ArrayList<>());
		bufferString.putIfAbsent(record.partition(), buffer);
		
		buffer.add(record.value());
		
		if (1 == buffer.size()) {	// 버퍼 첫 record인 경우
			currentFileOffset.put(record.partition(), record.offset());
		}
	}

	private void saveBufferToHdfsFile(Set<TopicPartition> partitions) {
		partitions.forEach(p -> flush(p.partition()));
	}

	private void flush(int partitionNo) {
		if (null != bufferString.get(partitionNo)) {
			if (bufferString.get(partitionNo).size() >= FLUSH_RECORD_COUNT) {
				save(partitionNo);
			}
		}
	}

	private void save(int partitionNo) {
		if (bufferString.get(partitionNo).size() > 0) {
			try {
				String fileName = String.format("/data/color-%d-%d.log", partitionNo, currentFileOffset.get(partitionNo));
			
				Configuration configuration = new Configuration();
				configuration.set("fs.defaultFs", HDFS_NAME);
				FileSystem hdfs = FileSystem.get(configuration);
				FSDataOutputStream fileOutputStream = hdfs.create(new Path(fileName));
				fileOutputStream.writeBytes(String.join("\n", bufferString.get(partitionNo)));
				fileOutputStream.close();
			} catch (IOException e) {
				log.error(e.getMessage(), e);
			}
			
		}
	}
	
	private void saveRemainBufferToHdfsFile() {
		bufferString.forEach((k ,v) -> this.save(k));
	}

	public void stopAndWakeup() {
		log.info("stop and wakeup");
		kafkaConsumer.wakeup();
		saveRemainBufferToHdfsFile();
	}
	
	
	
}
