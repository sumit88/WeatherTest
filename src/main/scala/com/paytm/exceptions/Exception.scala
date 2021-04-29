package com.paytm.exceptions

import org.apache.spark.SparkConf

case class SourceException(path: String) extends Exception {

  override def toString: String = {
    s"""Fatal Error Reading File : $path
         StackTrace : ${getStackTrace.mkString("\n")}

         Exiting Application
      """
  }

}

case class SparkSessionInitError(sparkConf: SparkConf) extends Exception {

  override def toString: String = {
    s""" Fatal Starting SparkSession with spark conf :
         ${sparkConf.getAll.mkString("\n")}

         StackTrace : ${getStackTrace.mkString("\n")}

         Exiting Application
      """
  }

}
