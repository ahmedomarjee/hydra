package hydra.sandbox.produce

import hydra.core.transport.{HydraRecord, DeliveryStrategy}

/**
  * Created by alexsilva on 3/29/17.
  */
case class FileRecord(destination: String, payload: String) extends HydraRecord[String, String] {

  override val key: Option[String] = None

  override val deliveryStrategy: DeliveryStrategy = DeliveryStrategy.AtMostOnce
}