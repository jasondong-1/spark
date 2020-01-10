package com.jason.exaples

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.ILoop
import scala.tools.nsc.Settings
object ILoopTest {
  def main(args: Array[String]): Unit = {
    val codeToRun = ":require /does/not/exist.jar"
    val output = ILoop.run(codeToRun)
  }
}
