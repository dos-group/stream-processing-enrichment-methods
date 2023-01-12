package org.myorg.OperatorsBase.TransactionEvent;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.mapping.Mapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Collector;
import org.myorg.Events.TransactionEvent.DeviceEvent;
import org.myorg.Events.TransactionEvent.LocationEvent;
import org.myorg.Events.TransactionEvent.TransactionEventRaw;

public class CassandraSinkManager {

	public static DataStream<LocationEvent> getLocationStream(DataStream<TransactionEventRaw> input) {
		return input.flatMap(new FlatMapFunction<TransactionEventRaw, LocationEvent>() {
			@Override
			public void flatMap(TransactionEventRaw value, Collector<LocationEvent> out) throws Exception {
				out.collect(value.getLocation());
			}
		});
	}

	public static DataStream<DeviceEvent> getDeviceStream(DataStream<TransactionEventRaw> input) {
		return input.flatMap(new FlatMapFunction<TransactionEventRaw, DeviceEvent>() {
			@Override
			public void flatMap(TransactionEventRaw value, Collector<DeviceEvent> out) throws Exception {
				out.collect(value.getDevice());
			}
		});
	}

	public static void addSink(
			String host,
			int port,
			String user,
			String password,
			DataStream input
	) throws Exception {
		CassandraSink.addSink(input)
				.setClusterBuilder(new ClusterBuilder() {
					@Override
					protected Cluster buildCluster(Cluster.Builder builder) {
						Cluster.Builder tempBuilder = builder.addContactPoint(host).withPort(port);
						tempBuilder.withCredentials(user, password);
						return tempBuilder.build();
					}
				})
				.setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
				.build();
	}




}
