package com.cloudera.flume.agent;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlumeNodeShutdownHook extends Thread {

  static final Logger LOG = LoggerFactory.getLogger(FlumeNodeShutdownHook.class);

  private FlumeNode flume;

  public FlumeNodeShutdownHook(FlumeNode flumeNode) {
    this.flume = flumeNode;
  }

  private void announceShutdown() {
    if (!LOG.isDebugEnabled()) {
      return;
    }

    LOG.debug("---------------------------------------------------------------------------");
    LOG.debug("---------------------------------------------------------------------------");
    LOG.debug("---------------------------------------------------------------------------");
    LOG.debug("SHUTTING DOWN NOW\n\n");

    Collection<LogicalNode> nodes = flume.getLogicalNodeManager().getNodes();
    for (LogicalNode node : nodes) {
      LOG.debug("Found node with properties: ");
      LOG.debug("  name: " + node.getName());
      LOG.debug("  src: " + node.getSource().getName());
      LOG.debug("  sink: " + node.getSink().getName());
    }

    LOG.debug("");
    LOG.debug("Going to stop() FlumeNode");

    LOG.debug("---------------------------------------------------------------------------");
    LOG.debug("---------------------------------------------------------------------------");
    LOG.debug("---------------------------------------------------------------------------");
  }

  @Override
  public void run() {
    announceShutdown();
    flume.stop();
  }

}
