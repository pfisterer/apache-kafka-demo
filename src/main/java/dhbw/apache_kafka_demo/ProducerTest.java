package dhbw.apache_kafka_demo;

import java.util.Properties;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractExecutionThreadService;

public class ProducerTest extends AbstractExecutionThreadService {
	private Logger log = LoggerFactory.getLogger(ProducerTest.class);
	private final KafkaProducer<String, String> producer;
	private final String topic;

	public ProducerTest(String server, String topic) {
		Properties props = new Properties();
		props.put("bootstrap.servers", server);
		props.put("group.id", "bla");
		props.put("client.id", this.getClass().getSimpleName());
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());

		this.topic = topic;
		producer = new KafkaProducer<String, String>(props);
	}

	public void run() {

		IntStream.range(1, 10).forEach(messageNo -> {

			// Create a message
			String messageStr = "Message_" + messageNo;

			// Send the message
			producer.send(new ProducerRecord<String, String>(topic, "" + messageNo, messageStr),
					(RecordMetadata metadata, Exception exception) -> {

						// Display some data about the message transmission
						if (metadata != null) {
							log.info("Sending message(" + messageNo + ", " + messageStr + ") sent to partition(" + metadata.partition()
									+ "), " + "offset(" + metadata.offset() + ")");
						} else {
							exception.printStackTrace();
						}

					});

			try {
				Thread.sleep(1000);
			} catch (Exception e) {
			}

		});

	}

}
