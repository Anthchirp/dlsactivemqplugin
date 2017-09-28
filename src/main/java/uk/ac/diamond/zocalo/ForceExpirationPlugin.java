/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.diamond.zocalo;

import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.Message;
import org.apache.activemq.filter.DestinationFilter;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

/**
 * A Broker interceptor which updates the expiration timestamp on a message
 * depending on predefined rules.
 *
 */
public class ForceExpirationPlugin extends BrokerPluginSupport {
//  private static final Logger LOG = LoggerFactory.getLogger(ForceExpirationPlugin.class);
    
    private static final DestinationFilter zocaloQ = DestinationFilter.parseFilter(
    		new ActiveMQQueue("zocalo.transient.>"));
    private static final DestinationFilter zocaloT = DestinationFilter.parseFilter(
    		new ActiveMQTopic("zocalo.transient.>"));
    private static final DestinationFilter zocdevQ = DestinationFilter.parseFilter(
                new ActiveMQQueue("zocdev.>"));
    private static final DestinationFilter zocdevT = DestinationFilter.parseFilter(
                new ActiveMQTopic("zocdev.>"));
    		
    /**
    * variable which (when non-zero) is used to override
    * the expiration date for messages that arrive with
    * no expiration date set (in Milliseconds).
    */
    long zeroExpirationOverride = 1 * 60 * 60 * 1000; // 1 hr

    /**
     * if true, update timestamp even if message has passed through a network
     * default false
     */
    boolean processNetworkMessages = false;

    /**
    * setter method for zeroExpirationOverride
    */
    public void setZeroExpirationOverride(long ttl)
    {
        this.zeroExpirationOverride = ttl;
    }

    public void setProcessNetworkMessages(Boolean processNetworkMessages) {
        this.processNetworkMessages = processNetworkMessages;
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message message) throws Exception {
        if (message.getTimestamp() > 0 && message.getExpiration() <= 0 && !isDestinationDLQ(message) &&
           (processNetworkMessages || (message.getBrokerPath() == null || message.getBrokerPath().length == 0))) {
            // timestamp not been disabled and has not passed through a network or processNetworkMessages=true

            ActiveMQDestination destination = message.getDestination();
            if (destination != null) {
                if (zocaloQ.matches(destination) || zocaloT.matches(destination) ||
                    zocdevQ.matches(destination) || zocdevT.matches(destination)) {
//                  LOG.debug("FEP: Seen message {} in applicable destination {}", new Object[]{
//                        message.getMessageId(),
//                        destination.getQualifiedName()
//                        });

                    long newTimeStamp = System.currentTimeMillis();
                    long timeToLive = zeroExpirationOverride;
//                  long oldTimestamp = message.getTimestamp();
                    message.setExpiration(newTimeStamp + timeToLive);
//                  LOG.debug("Set message {} expiration to {}", new Object[]{ message.getMessageId(), message.getExpiration() });
                }
            }
        }
        super.send(producerExchange, message);
    }

    private boolean isDestinationDLQ(Message message) {
      Destination regionDestination = (Destination) message.getRegionDestination();
      if (regionDestination == null)
        return false;

      DeadLetterStrategy deadLetterStrategy = regionDestination.getDeadLetterStrategy();
      if (deadLetterStrategy == null)
        return false;

      ActiveMQDestination originalDestination = message.getOriginalDestination();
      if (originalDestination == null)
        return false;

      // Cheap copy, since we only need two fields
      Message tmp = new ActiveMQMessage();
      tmp.setDestination(originalDestination);
      tmp.setRegionDestination(regionDestination);

      // Determine if we are headed for a DLQ
      ActiveMQDestination deadLetterDestination = deadLetterStrategy.getDeadLetterQueueFor(tmp, null);
      return deadLetterDestination.equals(message.getDestination());
    }
}
