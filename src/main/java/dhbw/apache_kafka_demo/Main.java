package dhbw.apache_kafka_demo;

import java.util.Set;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Service.State;
import com.google.common.util.concurrent.ServiceManager;

public class Main {

	public static void main(String[] args) throws Exception {
		// Parse command line options
		CommandLineOptions options = CommandLineOptions.parseCmdLineOptions(args);

		// Create a producer and consumer thread
		Set<Service> services = Sets.newHashSet();
		services.add(new ConsumerTest(options.zookeeper, options.topic));
		services.add(new ProducerTest(options.kafka, options.topic));

		// Start the threads
		ServiceManager serviceManager = new ServiceManager(services);
		serviceManager.startAsync().awaitHealthy();

		// Wait for the producer to finish
		if (serviceManager
				.servicesByState()
				.get(State.TERMINATED)
				.stream()
				.filter(service -> (service instanceof ProducerTest))
				.count() > 0) {
			serviceManager.stopAsync();
		}

		// Wait until all services are stopped
		serviceManager.awaitStopped();
	}

	public static class CommandLineOptions {
		@Option(name = "-zookeeper", usage = "Zookeeper address (e.g., localhost:2181)", required = false)
		public String zookeeper = "localhost:2181";

		@Option(name = "-kafka", usage = "Kafka broker address (e.g., localhost:9092)", required = false)
		public String kafka = "localhost:9092";

		@Option(name = "-topic", usage = "Topic to use", required = false)
		public String topic = "test1";

		@Option(name = "-v", aliases = {
				"--verbose" }, usage = "Verbose (DEBUG) logging output (default: INFO).", required = false)
		public boolean verbose = false;

		@Option(name = "-h", aliases = { "--help" }, usage = "This help message.", required = false)
		public boolean help = false;

		public static CommandLineOptions parseCmdLineOptions(final String[] args) {
			CommandLineOptions options = new CommandLineOptions();
			CmdLineParser parser = new CmdLineParser(options);

			try {
				parser.parseArgument(args);
				if (options.help)
					printHelpAndExit(parser);
			} catch (CmdLineException e) {
				System.err.println(e.getMessage());
				printHelpAndExit(parser);
			}

			return options;
		}

		private static void printHelpAndExit(CmdLineParser parser) {
			System.err.print("Usage:");
			parser.printSingleLineUsage(System.err);
			System.err.println();
			parser.printUsage(System.err);
			System.exit(1);
		}

	}

}
