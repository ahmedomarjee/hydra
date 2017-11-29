package hydra.core.transport

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import hydra.core.protocol.{RecordNotProduced, RecordProduced}
import hydra.core.test.{TestRecord, TestRecordMetadata}
import hydra.core.transport.TransportSupervisor.{Confirm, TransportError}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class TransportCallbackSpec extends TestKit(ActorSystem("test")) with Matchers with FunSpecLike with BeforeAndAfterAll
  with ImplicitSender {

  private val ingestor = TestProbe()
  private val supervisor = TestProbe()

  override def afterAll() {
    super.afterAll()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  describe("Transports Acks") {
    it("handles empty callbacks") {
      NoCallback.onCompletion(-1, None, Some(new IllegalArgumentException("test")))
      ingestor.expectNoMsg()
      supervisor.expectNoMsg()
    }

    it("handles simple/transport only callbacks") {
      val probe = TestProbe()
      new TransportSupervisorCallback(probe.ref).onCompletion(-11, None, Some(new IllegalArgumentException("test")))
      ingestor.expectNoMsg()
      supervisor.expectNoMsg()
      probe.expectMsg(TransportError(-11))

      new TransportSupervisorCallback(probe.ref).onCompletion(-11, Some(TestRecordMetadata(1)), None)
      ingestor.expectNoMsg()
      supervisor.expectNoMsg()
      probe.expectMsg(Confirm(-11))
    }

    it("handles ingestor callbacks") {
      val rec = TestRecord("OK", Some("1"), "test")
      val transport = TestProbe()
      val cb = new IngestorCallback[String, String](rec, ingestor.ref, supervisor.ref, transport.ref)

      cb.onCompletion(1, Some(TestRecordMetadata(1)), None)
      ingestor.expectMsgPF() {
        case RecordProduced(md, sup) =>
          sup shouldBe supervisor.ref
          md shouldBe a[TestRecordMetadata]
      }
      transport.expectMsg(Confirm(1))

      cb.onCompletion(1, None, Some(new IllegalArgumentException("test")))
      ingestor.expectMsgPF() {
        case RecordNotProduced(r, e, s) =>
          r shouldBe rec
          e.getMessage shouldBe "test"
          s shouldBe supervisor.ref
      }
      transport.expectMsg(TransportError(1))
    }
  }
}