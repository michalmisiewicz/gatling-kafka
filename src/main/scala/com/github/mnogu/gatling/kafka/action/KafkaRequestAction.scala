package com.github.mnogu.gatling.kafka.action

import com.github.mnogu.gatling.kafka.protocol.KafkaProtocol
import com.github.mnogu.gatling.kafka.request.builder.KafkaRequest
import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.util.DefaultClock
import io.gatling.commons.validation.Validation
import io.gatling.core.CoreComponents
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.core.session._
import io.gatling.core.util.NameGen
import org.apache.kafka.clients.producer._


class KafkaRequestAction[K,V](val producer: KafkaProducer[K,V],
                              val kafkaAttributes: KafkaRequest[K,V],
                              val coreComponents: CoreComponents,
                              val kafkaProtocol: KafkaProtocol,
                              val throttled: Boolean,
                              val next: Action )
  extends ExitableAction with NameGen {

  val statsEngine = coreComponents.statsEngine
  val clock = new DefaultClock
  override val name = genName("kafkaRequest")

  override def execute(session: Session): Unit = recover(session) {

    kafkaAttributes requestName session flatMap { requestName =>

      val outcome =
        sendRequest(
          requestName,
          producer,
          kafkaAttributes,
          throttled,
          session)

      outcome.onFailure(
        errorMessage =>
          statsEngine.reportUnbuildableRequest(session, requestName, errorMessage)
      )

      outcome

    }

  }

  private def sendRequest(requestName: String,
                          producer: Producer[K,V],
                          kafkaAttributes: KafkaRequest[K,V],
                          throttled: Boolean,
                          session: Session ): Validation[Unit] = {

      kafkaAttributes payload session map { payload =>

      val record = kafkaAttributes.key match {
        case Some(k) =>
          new ProducerRecord[K, V](kafkaProtocol.topic, k(session).toOption.get, payload)
        case None =>
          new ProducerRecord[K, V](kafkaProtocol.topic, payload)
      }

      if (throttled) {
        coreComponents.throttler.throttle(session.scenario, () => executeRequest(session, record, requestName, next))
      } else {
        executeRequest(session, record, requestName, next)
      }
    }

  }

  private def executeRequest(session: Session, record: ProducerRecord[K, V], requestName: String, next: Action): Unit = {
    val requestStartDate = clock.nowMillis

    producer.send(record, (_: RecordMetadata, e: Exception) => {
      val requestEndDate = clock.nowMillis
      statsEngine.logResponse(
        session,
        requestName,
        startTimestamp = requestStartDate,
        endTimestamp = requestEndDate,
        if (e == null) OK else KO,
        None,
        if (e == null) None else Some(e.getMessage)
      )
      next ! session
    })
  }
}
