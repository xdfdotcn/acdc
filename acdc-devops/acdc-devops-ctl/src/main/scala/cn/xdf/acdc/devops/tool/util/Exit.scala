package cn.xdf.acdc.devops.tool.util

object Exit {

  /**
   * Exiting the jvm.
   */
  def exit(code: Int, message: String): Unit = {
    System.exit(code)
  }

  /**
   * Exiting the jvm.
   */
  def exit(code: Int): Unit = {
    System.exit(code)
  }
}
