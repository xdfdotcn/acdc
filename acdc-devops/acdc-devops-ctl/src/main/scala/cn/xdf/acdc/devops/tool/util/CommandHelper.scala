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
package cn.xdf.acdc.devops.tool.util

import cn.xdf.acdc.devops.tool.command.CommandDefaultOptions
import joptsimple.{OptionParser, OptionSet, OptionSpec}

import java.util

object CommandHelper {
  /**
   * Check if there are no options or `--help` option from command line
   *
   * @param commandOpts Acceptable options for a command
   * @return true on matching the help check condition
   */
  def isPrintHelpNeeded(commandOpts: CommandDefaultOptions): Boolean = {
    commandOpts.args.length == 0 || commandOpts.options.has(commandOpts.helpOpt)
  }

  def isPrintVersionNeeded(commandOpts: CommandDefaultOptions): Boolean = {
    commandOpts.options.has(commandOpts.versionOpt)
  }

  /**
   * Check and print help message if there is no options or `--help` option
   * from command line, if `--version` is specified on the command line
   * print version information and exit.
   * NOTE: The function name is not strictly speaking correct anymore
   * as it also checks whether the version needs to be printed, but
   * refactoring this would have meant changing all command line tools
   * and unnecessarily increased the blast radius of this change.
   *
   * @param commandOpts Acceptable options for a command
   * @param message     Message to display on successful check
   */
  def printHelpAndExitIfNeeded(commandOpts: CommandDefaultOptions, message: String) = {
    if (isPrintHelpNeeded(commandOpts))
      printUsageAndDie(commandOpts.parser, message)
    if (isPrintVersionNeeded(commandOpts))
      printVersionAndDie()
  }

  /**
   * Check that all the listed options are present
   */
  def checkRequiredArgs(parser: OptionParser, options: OptionSet, required: OptionSpec[_]*): Unit = {
    for (arg <- required) {
      if (!options.has(arg))
        printUsageAndDie(parser, "Missing required argument \"" + arg + "\"")
    }
  }

  /**
   * Check whether the enumeration list  belongs to the given collection.
   */
  def checkEnumerations(parser: OptionParser, usedEnumerationList: Option[util.List[String]], enumerationSet: Set[String]): Unit = {
    usedEnumerationList.getOrElse(new util.ArrayList[String]())
      .forEach(it => {
        checkEnumeration(parser, Some(it), enumerationSet)
      })
  }

  /**
   * Check whether the enumeration belongs to the given collection.
   */
  def checkEnumeration(parser: OptionParser, usedEnumeration: Option[String], enumerationSet: Set[String]): Unit = {
    if (!enumerationSet.contains(usedEnumeration.orNull)) {
      printUsageAndDie(parser, "Incorrect enumeration value, options: \"" + usedEnumeration.orNull + "\"")
    }
  }

  /**
   * Check that none of the listed options are present
   */
  def checkInvalidArgs(parser: OptionParser, options: OptionSet, usedOption: OptionSpec[_], invalidOptions: Set[OptionSpec[_]]): Unit = {
    if (options.has(usedOption)) {
      for (arg <- invalidOptions) {
        if (options.has(arg))
          printUsageAndDie(parser, "Option \"" + usedOption + "\" can't be used with option \"" + arg + "\"")
      }
    }
  }

  /**
   * Check that none of the listed options are present with the combination of used options
   */
  def checkInvalidArgsSet(parser: OptionParser, options: OptionSet, usedOptions: Set[OptionSpec[_]], invalidOptions: Set[OptionSpec[_]],
                          trailingAdditionalMessage: Option[String] = None): Unit = {
    if (usedOptions.count(options.has) == usedOptions.size) {
      for (arg <- invalidOptions) {
        if (options.has(arg))
          printUsageAndDie(parser, "Option combination \"" + usedOptions.mkString(",") + "\" can't be used with option \"" + arg + "\"" + trailingAdditionalMessage.getOrElse(""))
      }
    }
  }

  /**
   * Print usage and exit
   */
  def printUsageAndDie(parser: OptionParser, message: String): Unit = {
    System.err.println(message)
    parser.printHelpOn(System.err)
    Exit.exit(1, message)
  }

  def printVersionAndDie(): Unit = {
    System.out.println(Version.version)
    Exit.exit(0)
  }
}
