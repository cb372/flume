package org.apache.flume.source;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroSource extends AbstractSource implements EventDrivenSource,
    Configurable, AvroSourceProtocol {

  private static final Logger logger = LoggerFactory
      .getLogger(AvroSource.class);

  private int port;
  private String bindAddress;

  private Server server;
  private CounterGroup counterGroup;

  public AvroSource() {
    counterGroup = new CounterGroup();
  }

  @Override
  public void configure(Context context) {
    port = context.get("port", Integer.class);
    bindAddress = context.get("bind", String.class);
  }

  @Override
  public void start() {
    logger.info("Avro source starting:{}", this);

    Responder responder = new SpecificResponder(AvroSourceProtocol.class, this);
    server = new NettyServer(responder,
        new InetSocketAddress(bindAddress, port));

    server.start();

    super.start();

    logger.debug("Avro source started");
  }

  @Override
  public void stop() {
    logger.info("Avro source stopping:{}", this);

    server.close();

    try {
      server.join();
    } catch (InterruptedException e) {
      logger
          .info("Interrupted while waiting for Avro server to stop. Exiting.");
    }

    super.stop();

    logger.debug("Avro source stopped. Metrics:{}", counterGroup);
  }

  @Override
  public String toString() {
    return "AvroSource: { bindAddress:" + bindAddress + " port:" + port + " }";
  }

  @Override
  public Status append(AvroFlumeEvent avroEvent) throws AvroRemoteException {
    logger.debug("Received avro event:{}", avroEvent);

    counterGroup.incrementAndGet("rpc.received");

    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();

      Map<String, String> headers = new HashMap<String, String>();

      for (Entry<CharSequence, CharSequence> entry : avroEvent.headers
          .entrySet()) {

        headers.put(entry.getKey().toString(), entry.getValue().toString());
      }

      Event event = EventBuilder.withBody(avroEvent.body.array(), headers);
      channel.put(event);
      counterGroup.incrementAndGet("rpc.events");

      transaction.commit();
    } catch (ChannelException e) {
      transaction.rollback();
      return Status.FAILED;
    } finally {
      transaction.close();
    }

    counterGroup.incrementAndGet("rpc.successful");

    return Status.OK;
  }
}