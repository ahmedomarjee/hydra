/*
 * Copyright (C) 2017 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.kafka.producer

import java.io.{File, InputStream}

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.TestKit
import com.pluralsight.hydra.avro.{InvalidDataTypeException, JsonConverter, RequiredFieldMissingException, UndefinedFieldsException}
import hydra.avro.JsonToAvroConversionExceptionWithMetadata
import hydra.avro.resource.SchemaResource
import hydra.core.akka.SchemaFetchActor.{FetchSchema, SchemaFetchResponse}
import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams._
import hydra.core.protocol.MissingMetadataException
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source

/**
  * Created by alexsilva on 1/11/17.
  */
class AvroRecordFactorySpec extends TestKit(ActorSystem("hydra"))
  with Matchers
  with FunSpecLike
  with ScalaFutures
  with BeforeAndAfterAll {

  val testSchema = Thread.currentThread()
    .getContextClassLoader.getResource("avro-factory-test.avsc").getFile

  val schemaResource = new SchemaResource {
    override def schema: Schema = new Schema.Parser().parse(new File(testSchema))

    override def id: Int = 1

    override def version: Int = 1

    override def location: String = "location"

    override def getDescription: String = "description"

    override def getInputStream: InputStream = null //not testing this
  }
  val loader = system.actorOf(Props(new Actor() {
    override def receive: Receive = {
      case FetchSchema(schema) => sender ! SchemaFetchResponse(schemaResource)
    }
  }))

  val factory = new AvroRecordFactory(loader)

  override def afterAll = TestKit.shutdownActorSystem(system)

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(1000 millis),
    interval = scaled(100 millis)
  )

  describe("When performing validation") {
    it("handles Avro default value errors") {
      val request = HydraRequest("123","""{"name":"test"}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:avro-factory-test.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val rec = factory.build(request)
      whenReady(rec.failed) { e =>
        val ex = e.asInstanceOf[JsonToAvroConversionExceptionWithMetadata]
        ex.getMessage should not be null
        ex.cause shouldBe an[RequiredFieldMissingException]
      }
    }

    it("handles fields not defined in the schema") {
      val request = HydraRequest("123","""{"name":"test","rank":1,"new-field":"new"}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:avro-factory-test.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
        .withMetadata(HYDRA_VALIDATION_STRATEGY -> "strict")
      val rec = new KafkaRecordFactories(loader).build(request)
      whenReady(rec.failed) { e =>
        val ex = e.asInstanceOf[JsonToAvroConversionExceptionWithMetadata]
        ex.cause shouldBe an[UndefinedFieldsException]
      }

    }

    it("handles Avro datatype errors") {
      val request = HydraRequest("123","""{"name":"test", "rank":"booyah"}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:avro-factory-test.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val rec = factory.build(request)
      whenReady(rec.failed) { e =>
        val ex = e.asInstanceOf[JsonToAvroConversionExceptionWithMetadata]
        ex.cause shouldBe an[InvalidDataTypeException]
      }
    }

    it("builds keyless messages") {
      val avroSchema = new Schema.Parser().parse(new File(testSchema))
      val json = """{"name":"test", "rank":10}"""
      val request = HydraRequest("123", json)
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:avro-factory-test.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      whenReady(factory.build(request)) { msg =>
        msg.destination shouldBe "test-topic"
        msg.key shouldBe None
        msg.schema shouldBe avroSchema
        msg.payload.get("name") shouldBe "test"
        msg.payload.get("rank") shouldBe 10
        msg.payload shouldBe new JsonConverter[GenericRecord](avroSchema).convert(json)
      }
    }

    it("builds keyed messages") {
      val avroSchema = new Schema.Parser().parse(new File(testSchema))
      val json = """{"name":"test", "rank":10}"""
      val request = HydraRequest("123", json)
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:avro-factory-test.avsc")
        .withMetadata(HYDRA_RECORD_KEY_PARAM -> "{$.name}")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      whenReady(factory.build(request)) { msg =>
        msg.destination shouldBe "test-topic"
        msg.schema shouldBe avroSchema
        msg.payload.get("name") shouldBe "test"
        msg.payload.get("rank") shouldBe 10
        msg.key shouldBe Some("test")
        msg.payload shouldBe new JsonConverter[GenericRecord](avroSchema).convert(json)
      }
    }

    it("has the right subject when a schema is specified") {
      val request = HydraRequest("123","""{"name":"test", "rank":10}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:avro-factory-test.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      factory.getTopicAndSchemaSubject(request).get._2 shouldBe "classpath:avro-factory-test.avsc"
    }

    it("defaults to target as the subject") {
      val request = HydraRequest("123","""{"name":"test", "rank":10}""")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      factory.getTopicAndSchemaSubject(request).get._2 shouldBe "test-topic"
    }

    it("throws an error if no topic is in the request") {
      val request = HydraRequest("123","""{"name":test"}""")
      whenReady(factory.build(request).failed)(_ shouldBe an[MissingMetadataException])
    }

    //validation
    it("returns invalid for payloads that do not conform to the schema") {
      val r = HydraRequest("1","""{"name":"test"}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:avro-factory-test.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      val rec = factory.build(r)
      whenReady(rec.failed)(_ shouldBe a[JsonToAvroConversionExceptionWithMetadata])
    }

    it("validates good avro payloads") {
      val r = HydraRequest("1","""{"name":"test","rank":10}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:avro-factory-test.avsc")
        .withMetadata(HYDRA_KAFKA_TOPIC_PARAM -> "test-topic")
      whenReady(factory.build(r)) { rec =>
        val avroSchema = new Schema.Parser()
          .parse(Source.fromResource("avro-factory-test.avsc").mkString)
        val genericRecord = new GenericRecordBuilder(avroSchema)
          .set("name", "test").set("rank", 10).build()
        val avroRecord = AvroRecord("test-topic", avroSchema, None, genericRecord)
        rec shouldBe avroRecord
      }
    }

  }
}
