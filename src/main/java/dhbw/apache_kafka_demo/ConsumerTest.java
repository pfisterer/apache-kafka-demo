package dhbw.apache_kafka_demo;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class ConsumerTest extends AbstractExecutionThreadService {
	private Logger log = LoggerFactory.getLogger(ConsumerTest.class);
	private String topicName;

	private ConsumerConfig config;

	public ConsumerTest(String server, String topicName) throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", server);
		props.put("zookeeper.connect", server);
		props.put("group.id", "bla");
		props.put("client.id", this.getClass().getSimpleName());
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		props.put("partition.assignment.strategy", "range");

		this.config = new ConsumerConfig(props);
		this.topicName = topicName;
	}

	public void run() {
		log.info("Starting");

		ConsumerConnector connector = Consumer.createJavaConsumerConnector(config);
		Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = connector.createMessageStreams(ImmutableMap.of(topicName, 1));
		List<KafkaStream<byte[], byte[]>> streams = messageStreams.get(topicName);
		ExecutorService executor = Executors.newFixedThreadPool(streams.size());

		for (KafkaStream<byte[], byte[]> stream : streams) {
			executor.submit(new Runnable() {
				public void run() {
					ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
					while (iterator.hasNext()) {
						MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();
						log.info("Received: {}", new String(messageAndMetadata.message()));
					}
				}
			});
		}

		log.info("Exiting");
	}

}
