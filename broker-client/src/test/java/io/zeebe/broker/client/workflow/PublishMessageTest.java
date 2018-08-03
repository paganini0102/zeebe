/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
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
package io.zeebe.broker.client.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import io.zeebe.broker.client.api.clients.WorkflowClient;
import io.zeebe.broker.client.api.events.MessageEvent;
import io.zeebe.broker.client.api.events.MessageState;
import io.zeebe.broker.client.cmd.ClientCommandRejectedException;
import io.zeebe.broker.client.cmd.ClientException;
import io.zeebe.broker.client.impl.data.MsgPackConverter;
import io.zeebe.broker.client.util.ClientRule;
import io.zeebe.msgpack.spec.MsgPackHelper;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.SubscriptionUtil;
import io.zeebe.protocol.intent.MessageIntent;
import io.zeebe.test.broker.protocol.brokerapi.ExecuteCommandRequest;
import io.zeebe.test.broker.protocol.brokerapi.StubBrokerRule;
import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

public class PublishMessageTest {
  public ClientRule clientRule = new ClientRule();
  public StubBrokerRule brokerRule = new StubBrokerRule();

  @Rule public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(clientRule);

  @Rule public ExpectedException expectedException = ExpectedException.none();

  private final MsgPackConverter msgPackConverter = new MsgPackConverter();

  private static final int FIRST_PARTITION = 1;
  private static final int PARTITION_SIZE = 10;

  private WorkflowClient workflowClient;

  @Before
  public void setUp() {

    brokerRule.workflowInstances().registerPublishMessageCommand();

    IntStream.range(FIRST_PARTITION, FIRST_PARTITION + PARTITION_SIZE)
        .forEach(partitionId -> brokerRule.addTopic("foo", partitionId));

    workflowClient = clientRule.getClient().topicClient("foo").workflowClient();
  }

  @Test
  public void shouldPublishMessage() {
    // when
    final MessageEvent messageEvent =
        workflowClient
            .newPublishMessageCommand()
            .messageName("order canceled")
            .correlationKey("order-123")
            .send()
            .join();

    // then
    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    assertThat(commandRequest.valueType()).isEqualTo(ValueType.MESSAGE);
    assertThat(commandRequest.intent()).isEqualTo(MessageIntent.PUBLISH);

    assertThat(commandRequest.getCommand())
        .containsOnly(
            entry("name", "order canceled"),
            entry("correlationKey", "order-123"),
            entry("payload", MsgPackHelper.EMTPY_OBJECT));

    assertThat(messageEvent.getState()).isEqualTo(MessageState.PUBLISHED);
    assertThat(messageEvent.getName()).isEqualTo("order canceled");
    assertThat(messageEvent.getCorrelationKey()).isEqualTo("order-123");
    assertThat(messageEvent.getMessageId()).isNull();
    assertThat(messageEvent.getPayload()).isEqualTo("{}");
    assertThat(messageEvent.getPayloadAsMap()).isEmpty();
  }

  @Test
  public void shouldPublishMessageWithId() {
    // when
    final MessageEvent messageEvent =
        workflowClient
            .newPublishMessageCommand()
            .messageName("order canceled")
            .correlationKey("order-123")
            .messageId("456")
            .send()
            .join();

    // then
    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    assertThat(commandRequest.getCommand())
        .containsOnly(
            entry("name", "order canceled"),
            entry("correlationKey", "order-123"),
            entry("messageId", "456"),
            entry("payload", MsgPackHelper.EMTPY_OBJECT));

    assertThat(messageEvent.getMessageId()).isEqualTo("456");
  }

  @Test
  public void shouldPublishMessageWithPayload() {
    // given
    final String payload = "{\"bar\":4}";

    // when
    final MessageEvent messageEvent =
        workflowClient
            .newPublishMessageCommand()
            .messageName("order canceled")
            .correlationKey("order-123")
            .payload(payload)
            .send()
            .join();

    // then
    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    assertThat(commandRequest.getCommand())
        .containsOnly(
            entry("name", "order canceled"),
            entry("correlationKey", "order-123"),
            entry("payload", msgPackConverter.convertToMsgPack(payload)));

    assertThat(messageEvent.getPayload()).isEqualTo(payload);
    assertThat(messageEvent.getPayloadAsMap()).containsOnly(entry("bar", 4));
  }

  @Test
  public void shouldPublishMessageWithIdAndPayload() {
    // given
    final String payload = "{\"bar\":4}";

    // when
    final MessageEvent messageEvent =
        workflowClient
            .newPublishMessageCommand()
            .messageName("order canceled")
            .correlationKey("order-123")
            .messageId("456")
            .payload(payload)
            .send()
            .join();

    // then
    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    assertThat(commandRequest.getCommand())
        .containsOnly(
            entry("name", "order canceled"),
            entry("correlationKey", "order-123"),
            entry("messageId", "456"),
            entry("payload", msgPackConverter.convertToMsgPack(payload)));

    assertThat(messageEvent.getName()).isEqualTo("order canceled");
    assertThat(messageEvent.getCorrelationKey()).isEqualTo("order-123");
    assertThat(messageEvent.getMessageId()).isEqualTo("456");
    assertThat(messageEvent.getPayload()).isEqualTo(payload);
  }

  @Test
  public void shouldPublishMessageWithPayloadAsStream() {
    // given
    final String payload = "{\"bar\":4}";

    // when
    final MessageEvent messageEvent =
        workflowClient
            .newPublishMessageCommand()
            .messageName("order canceled")
            .correlationKey("order-123")
            .payload(new ByteArrayInputStream(payload.getBytes()))
            .send()
            .join();

    // then
    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    assertThat(commandRequest.getCommand())
        .containsOnly(
            entry("name", "order canceled"),
            entry("correlationKey", "order-123"),
            entry("payload", msgPackConverter.convertToMsgPack(payload)));

    assertThat(messageEvent.getPayload()).isEqualTo(payload);
    assertThat(messageEvent.getPayloadAsMap()).containsOnly(entry("bar", 4));
  }

  @Test
  public void shouldPublishMessageWithPayloadAsMap() {
    // given
    final String payload = "{\"foo\":\"bar\"}";

    // when
    final MessageEvent messageEvent =
        workflowClient
            .newPublishMessageCommand()
            .messageName("order canceled")
            .correlationKey("order-123")
            .payload(Collections.singletonMap("foo", "bar"))
            .send()
            .join();

    // then
    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    assertThat(commandRequest.getCommand())
        .contains(entry("payload", msgPackConverter.convertToMsgPack(payload)));

    assertThat(messageEvent.getPayload()).isEqualTo(payload);
    assertThat(messageEvent.getPayloadAsMap()).containsOnly(entry("foo", "bar"));
  }

  @Test
  public void shouldPublishMessageWithPayloadAsObject() {
    // given
    final String payload = "{\"foo\":\"bar\"}";

    final PayloadObject payloadObj = new PayloadObject();
    payloadObj.foo = "bar";

    // when
    final MessageEvent messageEvent =
        workflowClient
            .newPublishMessageCommand()
            .messageName("order canceled")
            .correlationKey("order-123")
            .payload(payloadObj)
            .send()
            .join();

    // then
    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    assertThat(commandRequest.getCommand())
        .contains(entry("payload", msgPackConverter.convertToMsgPack(payload)));

    assertThat(messageEvent.getPayload()).isEqualTo(payload);
    assertThat(messageEvent.getPayloadAsMap()).containsOnly(entry("foo", "bar"));
    assertThat(messageEvent.getPayloadAsType(PayloadObject.class).foo).isEqualTo("bar");
  }

  @Test
  public void shouldPublishMessageOnSubscriptionPartition() {
    // given
    final int expectedPartition =
        FIRST_PARTITION
            + Math.abs(SubscriptionUtil.getSubscriptionHashCode("order-123") % PARTITION_SIZE);

    // when
    final MessageEvent messageEvent =
        workflowClient
            .newPublishMessageCommand()
            .messageName("order canceled")
            .correlationKey("order-123")
            .send()
            .join();

    // then
    final ExecuteCommandRequest commandRequest = brokerRule.getReceivedCommandRequests().get(0);
    assertThat(commandRequest.partitionId()).isEqualTo(expectedPartition);
    assertThat(messageEvent.getMetadata().getPartitionId()).isEqualTo(expectedPartition);
  }

  @Test
  public void shouldPublishMessagesOnSamePartition() {
    // given
    final int expectedPartition =
        FIRST_PARTITION
            + Math.abs(SubscriptionUtil.getSubscriptionHashCode("order-123") % PARTITION_SIZE);

    // when
    workflowClient
        .newPublishMessageCommand()
        .messageName("msg-1")
        .correlationKey("order-123")
        .send()
        .join();

    workflowClient
        .newPublishMessageCommand()
        .messageName("msg-2")
        .correlationKey("order-123")
        .send()
        .join();

    // then
    assertThat(brokerRule.getReceivedCommandRequests())
        .hasSize(2)
        .extracting(ExecuteCommandRequest::partitionId)
        .containsOnly(expectedPartition);
  }

  @Test
  public void shouldPublishMessagesOnDifferentPartitions() {
    // given
    final int expectedPartition1 =
        FIRST_PARTITION
            + Math.abs(SubscriptionUtil.getSubscriptionHashCode("order-123") % PARTITION_SIZE);

    final int expectedPartition2 =
        FIRST_PARTITION
            + Math.abs(SubscriptionUtil.getSubscriptionHashCode("order-456") % PARTITION_SIZE);

    assertThat(expectedPartition1).isNotEqualTo(expectedPartition2);

    // when
    workflowClient
        .newPublishMessageCommand()
        .messageName("msg-1")
        .correlationKey("order-123")
        .send()
        .join();

    workflowClient
        .newPublishMessageCommand()
        .messageName("msg-2")
        .correlationKey("order-456")
        .send()
        .join();

    // then
    assertThat(brokerRule.getReceivedCommandRequests())
        .hasSize(2)
        .extracting(ExecuteCommandRequest::partitionId)
        .containsExactly(expectedPartition1, expectedPartition2);
  }

  @Test
  public void shouldThrowExceptionOnRejection() {
    // given
    brokerRule.workflowInstances().registerPublishMessageCommand(r -> r.rejection());

    // expect exception
    expectedException.expect(ClientCommandRejectedException.class);
    expectedException.expectMessage("Command (PUBLISH) was rejected");

    // when
    workflowClient
        .newPublishMessageCommand()
        .messageName("order canceled")
        .correlationKey("order-123")
        .messageId("456")
        .send()
        .join();
  }

  @Test
  public void shouldThrowExceptionIfFailedToSerializePayload() {
    class NotSerializable {}

    // then
    expectedException.expect(ClientException.class);
    expectedException.expectMessage("Failed to serialize object");

    // when
    workflowClient
        .newPublishMessageCommand()
        .messageName("order canceled")
        .correlationKey("order-123")
        .payload(new NotSerializable())
        .send()
        .join();
  }

  @Test
  public void shouldThrowExceptionIfTopicDoesNotExist() {
    // then
    expectedException.expect(ClientException.class);
    expectedException.expectMessage("No topic found with name 'not-existing'");

    // when
    clientRule
        .getClient()
        .topicClient("not-existing")
        .workflowClient()
        .newPublishMessageCommand()
        .messageName("order canceled")
        .correlationKey("order-123")
        .send()
        .join();
  }

  public static class PayloadObject {
    public String foo;
  }

  public Map<String, Object> buildPartition(int id, String topic) {
    final Map<String, Object> partition = new HashMap<>();
    partition.put("topic", topic);
    partition.put("id", id);

    return partition;
  }
}