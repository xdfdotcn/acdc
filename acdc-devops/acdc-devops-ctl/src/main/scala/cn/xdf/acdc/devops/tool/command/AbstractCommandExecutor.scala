package cn.xdf.acdc.devops.tool.command

import java.util

abstract class AbstractCommandExecutor {

  def execute(token: Option[String]): Unit = {
    println()
    println()
    println()
    println()
    println("The command is being executed ....")
    println()
    println()
    println()
    println()
    println()

    val headers = new util.HashMap[String, Object]()
    headers.put("Authorization", "Bearer " + token.getOrElse(""))

    val result = doExecute(headers)

    println(result)
  }

  def doExecute(headers: util.Map[String, Object]): String

}
