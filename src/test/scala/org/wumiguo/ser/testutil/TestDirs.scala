package org.wumiguo.ser.testutil

/**
 * @author levinliu
 *         Created on 2020/6/16
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object TestDirs {

  def testOutputDir: String = {
    val testOutputDir = "test-output"
    val url = getClass().getClassLoader.getResource(testOutputDir)
    if (url == null) {
      throw new IllegalStateException("Please make sure the test-output dir exist, if not suggest to put a place holder file in it and build")
    }
    url.getPath
  }

  def resolveOutputPath(path: String) = {
    testOutputDir + (if (path.startsWith("/")) "" else "/") + path
  }

  def testDataDir: String = {
    val testOutputDir = "data"
    getClass().getClassLoader.getResource(testOutputDir).getPath
  }

  def resolveDataPath(path: String) = {
    testDataDir + (if (path.startsWith("/")) "" else "/") + path
  }

}
