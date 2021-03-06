package hydra.common

import com.typesafe.config.ConfigFactory
import hydra.common.auth.NoSecurityAuthenticator
import org.scalatest.{FlatSpec, Matchers}

class SettingsSpec extends FlatSpec with Matchers {
  it should "instantiate the configured authenticator" in {
    val config = ConfigFactory.parseString(
      """
        |http.authenticator = hydra.common.auth.NoSecurityAuthenticator
      """.stripMargin)
    new Settings(config).Authenticator shouldBe a[NoSecurityAuthenticator]
  }
}
