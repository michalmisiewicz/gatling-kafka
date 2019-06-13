package com.github.mnogu.gatling.kafka.action

import com.github.mnogu.gatling.kafka.protocol.{KafkaComponents, KafkaProtocol}
import com.github.mnogu.gatling.kafka.request.builder.KafkaRequest
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.structure.ScenarioContext
import org.apache.kafka.clients.producer.KafkaProducer


class KafkaRequestActionBuilder[K,V](producer: KafkaProducer[K, V], kafkaAttributes: KafkaRequest[K,V]) extends ActionBuilder {

  override def build( ctx: ScenarioContext, next: Action ): Action = {
    import ctx.{coreComponents, protocolComponentsRegistry, throttled}

    val kafkaComponents: KafkaComponents = protocolComponentsRegistry.components(KafkaProtocol.KafkaProtocolKey)

    new KafkaRequestAction(
      producer,
      kafkaAttributes,
      coreComponents,
      kafkaComponents.kafkaProtocol,
      throttled,
      next
    )

  }

}
