package cn.xdf.acdc.devops.tool.command

import cn.xdf.acdc.devops.tool.http.Ok
import cn.xdf.acdc.devops.tool.util.CommandHelper
import joptsimple.OptionSpec

import java.util
import scala.collection._

object ApiKafkaClusterCommand {
  def main(args: Array[String]): Unit = {
    val opts = new CommandOptions(args)
    opts.checkArgs()
    new CommandExecutor(opts).execute(opts.token)
  }

  class CommandExecutor(opts: CommandOptions) extends AbstractCommandExecutor {
    override def doExecute(headers: util.Map[String, Object]): String = {
      val resource = "/api/v1/admin/tools/api-kafka-cluster"

      val reqBody = new ReqBody
      reqBody.opt = opts.opt.orNull
      reqBody.bootstrapServer = opts.bootstrapServer.orNull
      reqBody.clusterType = opts.clusterType.orNull
      reqBody.securityProtocol = opts.securityProtocol.orNull

      if ("SASL_PLAINTEXT".equalsIgnoreCase(opts.securityProtocol.getOrElse("").trim)) {
        reqBody.saslMechanism = opts.saslMechanism.orNull
        reqBody.saslUsername = opts.saslUsername.orNull
        reqBody.saslPassword = opts.saslPassword.orNull
      }

      Ok.post(opts.apiServer.get, resource, reqBody, headers)
    }
  }

  class ReqBody {
    var opt: String = _;
    var bootstrapServer: String = _;
    var clusterType: String = _;
    var securityProtocol: String = _;
    var saslMechanism: String = _;
    var saslUsername: String = _;
    var saslPassword: String = _;
    var kafkaVersion = "2.6.0";
  }

  class CommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    // Help Documents
    private val createOpt = parser.accepts("create", "Create a kafka cluster.")
    private val deleteOpt = parser.accepts("delete", "Deleting kafka cluster.")
    private val getOpt = parser.accepts("get", "Get kafka cluster details.")


    private val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The Kafka server to connect to.")
      .withRequiredArg
      .describedAs("cluster server")
      .ofType(classOf[String])

    private val clusterTypeOpt = parser.accepts("cluster-type", "The cluster type, [INNER,TICDC].")
      .withRequiredArg
      .describedAs("cluster type")
      .ofType(classOf[String])

    private val securityProtocolOpt = parser.accepts("security-protocol", "The security protocol [SASL_PLAINTEXT,PLAINTEXT].")
      .withRequiredArg
      .describedAs("security protocol")
      .ofType(classOf[String])

    private val saslMechanismOpt = parser.accepts("sasl-mechanism", "The sasl mechanism, [SCRAM-SHA-512,SCRAM-SHA-256,PLAIN].")
      .withRequiredArg
      .describedAs("sasl mechanism")
      .ofType(classOf[String])

    private val saslUsernameOpt = parser.accepts("sasl-jaas-config-username", "The sasl username.")
      .withRequiredArg
      .describedAs("sasl username")
      .ofType(classOf[String])

    private val saslPasswordOpt = parser.accepts("sasl-jaas-config-password", "The sasl password.")
      .withRequiredArg
      .describedAs("sasl password")
      .ofType(classOf[String])

    options = parser.parse(args: _*)

    private val allActionOpts = immutable.Set[OptionSpec[_]](createOpt, deleteOpt, getOpt)

    private val clusterTypeEnumSet = immutable.Set[String]("INNER", "TICDC")
    private val securityProtocolEnumSet = immutable.Set[String]("SASL_PLAINTEXT", "PLAINTEXT")
    private val saslMechanismEnumSet = immutable.Set[String]("SCRAM-SHA-512", "SCRAM-SHA-256", "PLAIN")

    private def hasCreateOption: Boolean = has(createOpt)

    private def hasDeleteOption: Boolean = has(deleteOpt)

    private def hasGetOption: Boolean = has(getOpt)

    /**
     * Values.
     *
     */
    def opt: Option[String] = {
      if (hasCreateOption) return Some("CREATE")
      if (hasDeleteOption) return Some("DELETE")
      if (hasGetOption) return Some("GET")
      None
    }

    /**
     * Values.
     *
     */
    def bootstrapServer: Option[String] = valueAsOption(bootstrapServerOpt)

    def clusterType: Option[String] = valueAsOption(clusterTypeOpt)

    def securityProtocol: Option[String] = valueAsOption(securityProtocolOpt)

    def saslMechanism: Option[String] = valueAsOption(saslMechanismOpt)

    def saslUsername: Option[String] = valueAsOption(saslUsernameOpt)

    def saslPassword: Option[String] = valueAsOption(saslPasswordOpt)

    def doCheckArgs(): Unit = {
      val actions = Seq(createOpt, deleteOpt, getOpt).count(options.has)

      /**
       * Check action.
       */

      // action count
      if (actions != 1) CommandHelper.printUsageAndDie(parser, "Command must include exactly one action: , --create, --delete")

      // create
      if (hasCreateOption) {
        CommandHelper.checkRequiredArgs(parser, options, bootstrapServerOpt, clusterTypeOpt, securityProtocolOpt)
        CommandHelper.checkEnumeration(parser, securityProtocol, securityProtocolEnumSet)
        CommandHelper.checkEnumeration(parser, clusterType, clusterTypeEnumSet)

        if ("SASL_PLAINTEXT".equals(securityProtocol.getOrElse("").trim)) {
          CommandHelper.checkEnumeration(parser, saslMechanism, saslMechanismEnumSet)
          CommandHelper.checkRequiredArgs(parser, options, saslMechanismOpt, saslUsernameOpt, saslUsernameOpt)
        }
      }

      // delete
      if (hasDeleteOption) CommandHelper.checkRequiredArgs(parser, options, bootstrapServerOpt)


      // get
      if (hasGetOption) CommandHelper.checkRequiredArgs(parser, options, bootstrapServerOpt)

      /**
       * Check option.
       */

      // cluster type
      CommandHelper.checkInvalidArgs(parser, options, clusterTypeOpt, allActionOpts -- Set(createOpt))

      // security protocol
      CommandHelper.checkInvalidArgs(parser, options, securityProtocolOpt, allActionOpts -- Set(createOpt))

      // sasl mechanism
      CommandHelper.checkInvalidArgs(parser, options, saslMechanismOpt, allActionOpts -- Set(createOpt))

      // sasl jaas config username
      CommandHelper.checkInvalidArgs(parser, options, saslUsernameOpt, allActionOpts -- Set(createOpt))

      // sasl jaas config password
      CommandHelper.checkInvalidArgs(parser, options, saslPasswordOpt, allActionOpts -- Set(createOpt))
    }

    override def getLoginOpt: OptionSpec[_] = null
  }

}
