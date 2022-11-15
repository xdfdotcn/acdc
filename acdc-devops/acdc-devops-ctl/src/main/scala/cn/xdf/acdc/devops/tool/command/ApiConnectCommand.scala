package cn.xdf.acdc.devops.tool.command

import cn.xdf.acdc.devops.tool.http.Ok
import cn.xdf.acdc.devops.tool.util.CommandHelper
import joptsimple.OptionSpec

import java.util
import scala.collection._

object ApiConnectCommand {
  def main(args: Array[String]): Unit = {
    val opts = new CommandOptions(args)
    opts.checkArgs()
    new CommandExecutor(opts).execute(opts.token)
  }

  class ReqBody {
    var opt: String = _;
    var clusterType: String = _;
    var clusterServer: String = _;
    var schemaRegistryUrl: String = _;
    var key: String = _;
    var value: String = _;
    var securityProtocol: String = _;
    var saslMechanism: String = _;
  }

  class CommandExecutor(opts: CommandOptions) extends AbstractCommandExecutor {
    override def doExecute(headers: util.Map[String, Object]): String = {
      val resource = "/api/v1/admin/tools/api-connect"

      val reqBody = new ReqBody
      reqBody.opt = opts.opt.orNull
      reqBody.clusterServer = opts.clusterServer.orNull
      reqBody.clusterType = opts.clusterType.orNull
      reqBody.schemaRegistryUrl = opts.schemaRegistryUrl.orNull
      reqBody.key = opts.key.orNull
      reqBody.value = opts.value.orNull
      reqBody.securityProtocol = opts.securityProtocol.orNull
      reqBody.saslMechanism = opts.saslMechanism.orNull
      Ok.post(opts.apiServer.get, resource, reqBody, headers)
    }
  }

  class CommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    private val nl = System.getProperty("line.separator")

    // help
    private val createOpt = parser.accepts("create", "Creating a connect cluster.")
    private val deleteOpt = parser.accepts("delete", "Deleting a connect Cluster.")
    private val listOpt = parser.accepts("list", "Query the connect cluster list")
    private val getOpt = parser.accepts("get", "Get connect cluster details")
    private val updateDefaultConfigOpt = parser.accepts("update-default-config", "Update the default configuration of the connect cluster.")

    private val clusterServerOpt = parser.accepts("cluster-server", "The connect cluster service address.")
      .withRequiredArg
      .describedAs("cluster server")
      .ofType(classOf[String])

    private val clusterTypeOpt = parser.accepts("cluster-type", "The connect cluster type : [SOURCE_MYSQL,SOURCE_TIDB,SINK_MYSQL,SINK_TIDB,SINK_HIVE,SINK_KAFKA ].")
      .withRequiredArg
      .describedAs("cluster type")
      .ofType(classOf[String])

    private val schemaRegistryUrlOpt = parser.accepts("schema-registry-url", "The schema registry service address")
      .withRequiredArg
      .describedAs("schema registry url")
      .ofType(classOf[String])

    private val keyOpt = parser.accepts("key", "The Configuration item key")
      .withRequiredArg
      .describedAs("default config key")
      .ofType(classOf[String])
    private val valueOpt = parser.accepts("value", "The Configuration Item Value")
      .withRequiredArg
      .describedAs("default config value")
      .ofType(classOf[String])

    private val securityProtocolOpt = parser.accepts("security-protocol", "The security protocol [SASL_PLAINTEXT,PLAINTEXT].")
      .withRequiredArg
      .describedAs("security protocol")
      .ofType(classOf[String])

    private val saslMechanismOpt = parser.accepts("sasl-mechanism", "The sasl mechanism, [SCRAM-SHA-512,SCRAM-SHA-256,PLAIN].")
      .withRequiredArg
      .describedAs("sasl mechanism")
      .ofType(classOf[String])

    options = parser.parse(args: _*)

    private val allActionOpts = immutable.Set[OptionSpec[_]](createOpt, listOpt, getOpt, updateDefaultConfigOpt, deleteOpt)

    private val securityProtocolEnumSet = immutable.Set[String]("SASL_PLAINTEXT", "PLAINTEXT")

    private val saslMechanismEnumSet = immutable.Set[String]("SCRAM-SHA-512", "SCRAM-SHA-256", "PLAIN")

    private val clusterTypeEnumerationSet = immutable.Set[String]("SOURCE_MYSQL", "SOURCE_TIDB", "SINK_MYSQL", "SINK_TIDB", "SINK_HIVE", "SINK_KAFKA")

    private val sourceClusterTypeEnumerationSet = immutable.Set[String]("SOURCE_MYSQL", "SOURCE_TIDB")

    private def hasCreateOption: Boolean = has(createOpt)

    private def hasListOption: Boolean = has(listOpt)

    private def hasGetOption: Boolean = has(getOpt)

    private def hasUpdateDefaultConfigOption: Boolean = has(updateDefaultConfigOpt)

    private def hasDeleteOption: Boolean = has(deleteOpt)

    /**
     * Values.
     *
     */

    def opt: Option[String] = {
      // CREATE, DELETE, UPDATE_DEFAULT_CONFIG, GET, LIST
      if (hasCreateOption) return Some("CREATE")
      if (hasListOption) return Some("LIST")
      if (hasGetOption) return Some("GET")
      if (hasUpdateDefaultConfigOption) return Some("UPDATE_DEFAULT_CONFIG")
      if (hasDeleteOption) return Some("DELETE")
      None
    }

    def clusterType: Option[String] = valueAsOption(clusterTypeOpt)

    def key: Option[String] = valueAsOption(keyOpt)

    def value: Option[String] = valueAsOption(valueOpt)

    def clusterServer: Option[String] = valueAsOption(clusterServerOpt)

    def schemaRegistryUrl: Option[String] = valueAsOption(schemaRegistryUrlOpt)

    def securityProtocol: Option[String] = valueAsOption(securityProtocolOpt)

    def saslMechanism: Option[String] = valueAsOption(saslMechanismOpt)


    def doCheckArgs(): Unit = {
      val actions = Seq(createOpt, listOpt, getOpt, updateDefaultConfigOpt, deleteOpt).count(options.has)

      /**
       * Check action  .
       */

      // action count
      if (actions != 1) CommandHelper.printUsageAndDie(parser, "Command must include exactly one action: --create, --list, --get, --update-default-config or --delete")

      // create
      if (hasCreateOption) {
        CommandHelper.checkRequiredArgs(parser, options, clusterTypeOpt, clusterServerOpt, schemaRegistryUrlOpt)
        CommandHelper.checkEnumeration(parser, clusterType, clusterTypeEnumerationSet)

        if (sourceClusterTypeEnumerationSet.contains(clusterType.getOrElse(""))) {
          CommandHelper.checkRequiredArgs(parser, options, securityProtocolOpt)
          CommandHelper.checkEnumeration(parser, securityProtocol, securityProtocolEnumSet)
          if ("SASL_PLAINTEXT".equals(securityProtocol.getOrElse(""))) {
            CommandHelper.checkRequiredArgs(parser, options, saslMechanismOpt)
            CommandHelper.checkEnumeration(parser, saslMechanism, saslMechanismEnumSet)
          }
        }
      }

      // list
      if (hasListOption) CommandHelper.checkRequiredArgs(parser, options)

      // get
      if (hasGetOption) CommandHelper.checkRequiredArgs(parser, options, clusterServerOpt)

      // update default config
      if (hasUpdateDefaultConfigOption) CommandHelper.checkRequiredArgs(parser, options, clusterServerOpt, keyOpt, valueOpt)

      // delete
      if (hasDeleteOption) CommandHelper.checkRequiredArgs(parser, options, clusterServerOpt)

      /**
       * Check option.
       */

      // cluster type
      CommandHelper.checkInvalidArgs(parser, options, clusterTypeOpt, allActionOpts -- Set(createOpt))

      // cluster server
      CommandHelper.checkInvalidArgs(parser, options, clusterServerOpt, allActionOpts -- Set(createOpt, getOpt, deleteOpt, updateDefaultConfigOpt))

      // schema registry url
      CommandHelper.checkInvalidArgs(parser, options, schemaRegistryUrlOpt, allActionOpts -- Set(createOpt))

      // default config key
      CommandHelper.checkInvalidArgs(parser, options, keyOpt, allActionOpts -- Set(updateDefaultConfigOpt))

      // default config value
      CommandHelper.checkInvalidArgs(parser, options, valueOpt, allActionOpts -- Set(updateDefaultConfigOpt))

      // security protocol
      CommandHelper.checkInvalidArgs(parser, options, securityProtocolOpt, allActionOpts -- Set(createOpt))

      // sasl mechanism
      CommandHelper.checkInvalidArgs(parser, options, saslMechanismOpt, allActionOpts -- Set(createOpt))
    }

    override def getLoginOpt: OptionSpec[_] = null
  }

}
