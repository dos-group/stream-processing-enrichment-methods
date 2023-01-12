/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.OperatorsBase;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.shaded.guava30.com.google.common.hash.HashCode;
import org.apache.flink.shaded.guava30.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava30.com.google.common.hash.Hashing;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.myorg.EnrichmentMethods.EnrichmentFactory;
import org.myorg.Events.TransactionEvent.TransactionEventRaw;
import org.myorg.Events.TransactionEvent.TransactionResult;
import org.myorg.OperatorsBase.TransactionEvent.LatencyCalculator;
import org.myorg.OperatorsBase.TransactionEvent.TransactionSourceSink;
import org.myorg.OperatorsBase.TransactionEvent.TransactionWindow;

import java.time.Duration;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class FraudDetection {

	private static final int WINDOW_SIZE_S = 10;
	private static final int WINDOW_SLIDE_S = 5;
	private static final boolean LOG_LATENCIES = true;

	private static String getOperatorId(String uid) {
		HashFunction hashFunction = Hashing.murmur3_128();
		HashCode hashCode = hashFunction.hashBytes(uid.getBytes());
		return hashCode.toString();
	}

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(600_000);

		// create external config with program arguments
		ParameterTool parameters = ParameterTool.fromArgs(args);
		ExternalConfiguration config = new ExternalConfiguration(parameters);

		KafkaSource<TransactionEventRaw> source = TransactionSourceSink.getTransactionEventsSource(
				config.getKafkaHost(), config.getKafkaPort(), config.getKafkaInputTopic()
		);
		DataStream<TransactionEventRaw> input = env.fromSource(
				source,
				WatermarkStrategy
						.<TransactionEventRaw>forBoundedOutOfOrderness(Duration.ofMillis(1000))
						.withTimestampAssigner(((element, recordTimestamp) -> element.getTransaction().getTimestamp()))
						.withIdleness(Duration.ofSeconds(1)),
				"Kafka-Source"
		);

		// enrich events
		DataStream<TransactionEventRaw> resultStream = EnrichmentFactory.enrich(
				env, input, parameters.get("enrichmentType", "sync"), config, LOG_LATENCIES
		);

		// add window start timestamp
		resultStream = resultStream.flatMap(new FlatMapFunction<TransactionEventRaw, TransactionEventRaw>() {
			@Override
			public void flatMap(TransactionEventRaw value, Collector<TransactionEventRaw> out) throws Exception {
				value.setWindowStartMeasure(System.currentTimeMillis());
				out.collect(value);
			}
		}).name("Add-Window-Start-Time");

		// compute transaction amount for each account in last window size seconds
		DataStream<TransactionResult> output = resultStream.keyBy(TransactionEventRaw::getKey)
				.window(SlidingProcessingTimeWindows.of(Time.seconds(WINDOW_SIZE_S), Time.seconds(WINDOW_SLIDE_S)))
				.apply(new TransactionWindow()).name("Sliding-Window-Sum-Transaction-Amounts");

		// calculate total latency.
		output = output
				.keyBy(TransactionResult::getWindowStart)
				.flatMap(new LatencyCalculator(WINDOW_SIZE_S)).name("Add-Total-Latency");

		KafkaSink<TransactionResult> sink = TransactionSourceSink.getKafkaSink(
				config.getKafkaHost(), config.getKafkaPort(), config.getKafkaOutputTopic()
		);
		output.sinkTo(sink);

 		env.execute("Stream-Job-EnrichmentMethods");
	}

}
