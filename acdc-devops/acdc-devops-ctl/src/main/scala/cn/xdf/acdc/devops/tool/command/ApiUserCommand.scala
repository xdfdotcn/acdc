package cn.xdf.acdc.devops.tool.command

import cn.xdf.acdc.devops.tool.http.Ok
import cn.xdf.acdc.devops.tool.util.CommandHelper
import joptsimple.OptionSpec

import java.util
import scala.collection._

object ApiUserCommand {
  def main(args: Array[String]): Unit = {
    val opts = new CommandOptions(args)
    opts.checkArgs()
    new CommandExecutor(opts).execute(opts.token)
  }

  class CommandExecutor(opts: CommandOptions) extends AbstractCommandExecutor {
    override def doExecute(headers: util.Map[String, Object]): String = {
      val loginResource = "/login"
      val resource = "/api/v1/admin/tools/api-user"

      val reqBody = new ReqBody
      reqBody.opt = opts.opt.orNull
      reqBody.username = opts.username.orNull
      reqBody.email = opts.email.orNull
      reqBody.password = opts.password.orNull
      reqBody.roles = opts.role.orNull
      reqBody.begin = opts.begin.orNull
      reqBody.pagesize = opts.pagesize.orNull
      reqBody.oldPassword = opts.oldPassword.orNull
      reqBody.newPassword = opts.newPassword.orNull

      if ("LOGIN".equals(reqBody.opt)) {
        val params = new util.HashMap[String, Object]()
        params.put("username", reqBody.email)
        params.put("password", reqBody.password)
        Ok.post(opts.apiServer.get, loginResource, params, headers)
      } else {
        Ok.post(opts.apiServer.get, resource, reqBody, headers)
      }
    }
  }


  class ReqBody {
    var opt: String = _;
    var username: String = _;
    var email: String = _;
    var password: String = _;
    var roles: util.List[String] = _;
    var begin: Integer = _;
    var pagesize: Integer = _;
    var oldPassword: String = _;
    var newPassword: String = _;
  }

  class CommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    // Help Documents
    private val loginOpt = parser.accepts("login", "Login to the system and obtain an access token.")
    private val createOpt = parser.accepts("create", "Create a new user.")
    private val deleteOpt = parser.accepts("delete", "Deleting a user.")
    private val updateOpt = parser.accepts("update", "Modifying a name of user.")
    private val getOpt = parser.accepts("get", "Get user details.")
    private val listOpt = parser.accepts("list", "Querying the user list.")
    private val resetPasswordOpt = parser.accepts("reset-password", "Reset the password.")
    private val resetRoleOpt = parser.accepts("reset-role", "Reset the role.")


    private val usernameOpt = parser.accepts("username", "The username.")
      .withRequiredArg
      .describedAs("username")
      .ofType(classOf[String])

    private val emailOpt = parser.accepts("email", "The email.")
      .withRequiredArg
      .describedAs("email")
      .ofType(classOf[String])

    private val passwordOpt = parser.accepts("password", "The password.")
      .withRequiredArg
      .describedAs("password")
      .ofType(classOf[String])

    private val roleOpt = parser.accepts("role", "The role.")
      .withRequiredArg
      .describedAs("role")
      .ofType(classOf[String])

    private val beginOpt = parser.accepts("begin", "The Start page number.")
      .withRequiredArg
      .describedAs("start page")
      .ofType(classOf[java.lang.Integer])

    private val pagesizeOpt = parser.accepts("pagesize", "The Size per page.")
      .withRequiredArg
      .describedAs("pagesize")
      .ofType(classOf[java.lang.Integer])


    private val oldPasswordOpt = parser.accepts("old-password", "The old password.")
      .withRequiredArg
      .describedAs("old password")
      .ofType(classOf[String])

    private val newPasswordOpt = parser.accepts("new-password", "The new password.")
      .withRequiredArg
      .describedAs("new password")
      .ofType(classOf[String])

    options = parser.parse(args: _*)

    private val allActionOpts = immutable.Set[OptionSpec[_]](loginOpt, createOpt, deleteOpt, updateOpt, getOpt, listOpt, resetPasswordOpt, resetRoleOpt)

    private val roleTypeEnumerationSet = immutable.Set[String]("ROLE_ADMIN", "ROLE_DBA", "ROLE_USER")

    private def hasLoginOption: Boolean = has(loginOpt)

    private def hasCreateOption: Boolean = has(createOpt)

    private def hasDeleteOption: Boolean = has(deleteOpt)

    private def hasUpdateOption: Boolean = has(updateOpt)

    private def hasGetOption: Boolean = has(getOpt)

    private def hasListOption: Boolean = has(listOpt)

    private def hasResetPasswordOption: Boolean = has(resetPasswordOpt)

    private def hasResetRoleOption: Boolean = has(resetRoleOpt)

    /**
     * Values.
     *
     */
    def opt: Option[String] = {
      // LOGIN, CREATE, DELETE, UPDATE, GET,LIST,RESET_PASSWORD,RESET_ROLE
      if (hasLoginOption) return Some("LOGIN")
      if (hasCreateOption) return Some("CREATE")
      if (hasDeleteOption) return Some("DELETE")
      if (hasUpdateOption) return Some("UPDATE")
      if (hasGetOption) return Some("GET")
      if (hasListOption) return Some("LIST")
      if (hasResetPasswordOption) return Some("RESET_PASSWORD")
      if (hasResetRoleOption) return Some("RESET_ROLE")
      None
    }

    /**
     * new-password
     *
     */
    def username: Option[String] = valueAsOption(usernameOpt)

    def email: Option[String] = valueAsOption(emailOpt)

    def password: Option[String] = valueAsOption(passwordOpt)

    //    def role: Option[String] = valueAsOption(roleOpt)
    def role: Option[util.List[String]] = valuesAsOption(roleOpt)


    def begin: Option[Integer] = valueAsOption(beginOpt)

    def pagesize: Option[Integer] = valueAsOption(pagesizeOpt)

    def oldPassword: Option[String] = valueAsOption(oldPasswordOpt)

    def newPassword: Option[String] = valueAsOption(newPasswordOpt)

    def doCheckArgs(): Unit = {
      val actions = Seq(loginOpt, createOpt, deleteOpt, updateOpt, getOpt, listOpt, resetPasswordOpt, resetRoleOpt).count(options.has)

      /**
       * Check action.
       */

      // action count
      if (actions != 1) CommandHelper.printUsageAndDie(parser, "Command must include exactly one action: --login, --create, --delete, --update, --get, --list, --reset-password, --reset-role")

      // login
      if (hasLoginOption) CommandHelper.checkRequiredArgs(parser, options, emailOpt, passwordOpt)

      // create
      if (hasCreateOption) {
        CommandHelper.checkRequiredArgs(parser, options, usernameOpt, emailOpt, passwordOpt, roleOpt)
        CommandHelper.checkEnumerations(parser, role, roleTypeEnumerationSet)
      }

      // delete
      if (hasDeleteOption) CommandHelper.checkRequiredArgs(parser, options, emailOpt)

      // update
      if (hasUpdateOption) CommandHelper.checkRequiredArgs(parser, options, usernameOpt, emailOpt)

      // get
      if (hasGetOption) CommandHelper.checkRequiredArgs(parser, options, emailOpt)

      // list
      if (hasListOption) CommandHelper.checkRequiredArgs(parser, options, beginOpt, pagesizeOpt)

      // reset password
      if (hasResetPasswordOption) CommandHelper.checkRequiredArgs(parser, options, emailOpt, oldPasswordOpt, newPasswordOpt)

      // reset role
      if (hasResetRoleOption) CommandHelper.checkRequiredArgs(parser, options, emailOpt, roleOpt)

      /**
       * Check option.
       *
       * username  create update
       * email  create  delete update get reset-password reset-role
       * password  create login
       * role create  reset-role
       * begin list
       * pagesize list
       * old-password  reset-password
       * new-password reset-password
       */

      // username
      CommandHelper.checkInvalidArgs(parser, options, usernameOpt, allActionOpts -- Set(createOpt, updateOpt))

      // email
      CommandHelper.checkInvalidArgs(parser, options, emailOpt, allActionOpts -- Set(createOpt, deleteOpt, updateOpt, getOpt, resetPasswordOpt, resetRoleOpt, loginOpt))

      // password
      CommandHelper.checkInvalidArgs(parser, options, passwordOpt, allActionOpts -- Set(createOpt, loginOpt))

      // role
      CommandHelper.checkInvalidArgs(parser, options, roleOpt, allActionOpts -- Set(createOpt, resetRoleOpt))

      // begin
      CommandHelper.checkInvalidArgs(parser, options, beginOpt, allActionOpts -- Set(listOpt))

      // pagesize
      CommandHelper.checkInvalidArgs(parser, options, pagesizeOpt, allActionOpts -- Set(listOpt))

      // oldPassword
      CommandHelper.checkInvalidArgs(parser, options, oldPasswordOpt, allActionOpts -- Set(resetPasswordOpt))

      // newPassword
      CommandHelper.checkInvalidArgs(parser, options, newPasswordOpt, allActionOpts -- Set(resetPasswordOpt))
    }

    override def getLoginOpt: OptionSpec[_] = loginOpt
  }
}
