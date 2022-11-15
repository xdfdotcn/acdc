/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.xdf.acdc.devops.tool.command

import cn.xdf.acdc.devops.tool.util.CommandHelper

import java.util
import joptsimple.{OptionParser, OptionSet, OptionSpec}

abstract class CommandDefaultOptions(val args: Array[String], allowCommandOptionAbbreviation: Boolean = false) {
  val parser = new OptionParser(allowCommandOptionAbbreviation)

  val apiServerOpt = parser.accepts("api-server", "REQUIRED: The API server to connect to.")
    .withRequiredArg
    .describedAs("api server")
    .ofType(classOf[String])

  val tokenOpt = parser.accepts("token", "REQUIRED: The access token, you can obtain the value from the acdc-api-user --login command.")
    .withRequiredArg
    .describedAs("access token")
    .ofType(classOf[String])

  val helpOpt = parser.accepts("help", "Print usage information.").forHelp()

  val versionOpt = parser.accepts("version", "Display ACDC version.").forHelp()

  var options: OptionSet = _

  def has(builder: OptionSpec[_]): Boolean = options.has(builder)

  def valueAsOption[A](option: OptionSpec[A], defaultValue: Option[A] = None): Option[A] = if (has(option)) Some(options.valueOf(option)) else defaultValue

  def valuesAsOption[A](option: OptionSpec[A], defaultValue: Option[util.List[A]] = None): Option[util.List[A]] = if (has(option)) Some(options.valuesOf(option)) else defaultValue

  // value
  def apiServer: Option[String] = valueAsOption(apiServerOpt)

  def token: Option[String] = valueAsOption(tokenOpt)

  def checkArgs(): Unit = {
    // help and version
    CommandHelper.printHelpAndExitIfNeeded(this, "ACDC metadata management tool.")
    CommandHelper.checkRequiredArgs(parser, options, apiServerOpt)
    if (!has(getLoginOpt)) CommandHelper.checkRequiredArgs(parser, options, tokenOpt)
    doCheckArgs
  }

  def doCheckArgs

  def getLoginOpt: OptionSpec[_]
}
