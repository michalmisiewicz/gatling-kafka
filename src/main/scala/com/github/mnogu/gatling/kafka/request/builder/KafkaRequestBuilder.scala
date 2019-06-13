package com.github.mnogu.gatling.kafka.request.builder

import com.github.mnogu.gatling.kafka.action.KafkaRequestActionBuilder
import io.gatling.core.session._
import org.apache.kafka.clients.producer.KafkaProducer

case class KafkaRequest[K,V](requestName: Expression[String],
                             key: Option[Expression[K]],
                             payload: Expression[V] )

case class KafkaRequestBuilder(requestName: Expression[String]) {

  def send[V](payload: Expression[V])(implicit producer: KafkaProducer[_, V]): KafkaRequestActionBuilder[_,V] = send(payload, None, producer)

  def send[K,V](key: Expression[K], payload: Expression[V])(implicit producer: KafkaProducer[K, V]): KafkaRequestActionBuilder[K,V] = send(payload, Some(key), producer)

  private def send[K,V](payload: Expression[V], key: Option[Expression[K]], producer: KafkaProducer[K, V]) =
    new KafkaRequestActionBuilder(producer, KafkaRequest(requestName, key, payload))
}
