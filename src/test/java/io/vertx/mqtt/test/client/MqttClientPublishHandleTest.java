/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vertx.mqtt.test.client;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

/**
 * MQTT client testing on handling messages
 * Also testing manual message handling
 */
@RunWith(VertxUnitRunner.class)
public class MqttClientPublishHandleTest {

  private static final Logger log = LoggerFactory.getLogger(MqttClientPublishHandleTest.class);

  private static final String MQTT_TOPIC = "/my_topic";
  private static final String MQTT_MESSAGE = "Hello Vert.x MQTT Client";

  private int messageId = 0;

  @Test
  public void publishQoS2(TestContext context) throws InterruptedException {
    Async async = context.async(2);
    Vertx vertx = Vertx.vertx();
    this.subscribeAndHandle(vertx, context, async, MqttQoS.EXACTLY_ONCE, false)
      .compose(v -> this.publish(vertx, context, async, MqttQoS.EXACTLY_ONCE));


    async.await();
  }

  @Test
  public void publishQoS1(TestContext context) throws InterruptedException {
    Async async = context.async(2);
    Vertx vertx = Vertx.vertx();
    this.subscribeAndHandle(vertx, context, async, MqttQoS.AT_LEAST_ONCE, false)
      .compose(v -> this.publish(vertx, context, async, MqttQoS.AT_LEAST_ONCE));

    async.await();
  }

  @Test
  public void publishQoS1ManualHandshake(TestContext context) throws InterruptedException {
    Async async = context.async(2);
    Vertx vertx = Vertx.vertx();
    this.subscribeAndHandle(vertx, context, async, MqttQoS.AT_LEAST_ONCE, true)
      .compose(v -> this.publish(vertx, context, async, MqttQoS.AT_LEAST_ONCE));

    async.await();
  }

  @Test
  public void publishQoS2ManualHandshake(TestContext context) throws InterruptedException {
    Async async = context.async(2);
    Vertx vertx = Vertx.vertx();
    this.subscribeAndHandle(vertx, context, async, MqttQoS.EXACTLY_ONCE, true)
      .compose(v -> this.publish(vertx, context, async, MqttQoS.EXACTLY_ONCE));

    async.await();
  }

  private Future<Void> subscribeAndHandle(Vertx vertx, TestContext context, Async async, MqttQoS qos, boolean manualHandshake) {

    Future<Void> subscribeResult = Future.future();

    MqttClient client = MqttClient.create(vertx);

    if(manualHandshake) {
      switch (qos) {

        case AT_LEAST_ONCE:
          client.publishHandlerManualHandshake(msg -> {
            log.info("Publish message with id " + msg.messageId() + " received, acknowledging");
            client.publishAcknowledge(msg.messageId());
            client.disconnect();
            async.countDown();
          });
          break;

        case EXACTLY_ONCE:
          client.pubrelHandler(msg -> {
            log.info("Pubrel message for message with id " + msg.messageId() + " received, sending pubcomp");
            client.publishComplete(msg.messageId());
            client.disconnect();
            async.countDown();
          })
          .publishHandlerManualHandshake(msg -> {
           log.info("Publish message with id " + msg.messageId() + " received, sending pubrec");
           client.publishReceived(msg);
          });
          break;
      }
    } else {
      client.publishHandler(msg -> {
        log.info("Publish message with id " + msg.messageId() + " received, auto acknowledged");
        client.disconnect();
        async.countDown();
      });
    }

    client.subscribeCompletionHandler(suback -> {
      subscribeResult.complete();
    });

    client.connect(MqttClientOptions.DEFAULT_PORT, TestUtil.BROKER_ADDRESS, ar -> {
      assertTrue(ar.succeeded());
      client.subscribe(MQTT_TOPIC, qos.value());
    });

    return subscribeResult;
  }

  private Future<Void> publish(Vertx vertx, TestContext context, Async async, MqttQoS qos) {

    Future<Void> publishResult = Future.future();

    this.messageId = 0;

    MqttClient client = MqttClient.create(vertx);

    client.connect(MqttClientOptions.DEFAULT_PORT, TestUtil.BROKER_ADDRESS, ar -> {

      assertTrue(ar.succeeded());

      client.publish(
        MQTT_TOPIC,
        Buffer.buffer(MQTT_MESSAGE.getBytes()),
        qos,
        false,
        false,
        ar1 -> {
          assertTrue(ar.succeeded());
          messageId = ar1.result();
          log.info("publishing message id = " + messageId);
        }
      );
    });

    client.publishCompletionHandler(pubid -> {
      assertTrue(pubid == messageId);
      log.info("publishing complete for message id = " + pubid);
      publishResult.complete();
      client.disconnect();
      async.countDown();
    });

    return publishResult;
  }
}
