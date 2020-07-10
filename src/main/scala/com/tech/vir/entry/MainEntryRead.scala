package com.tech.vir.entry

import com.tech.vir.session.DemoSparkSession

object MainEntryRead {

  def main(args: Array[String]): Unit = {

    val spark = DemoSparkSession.sparkSession()

    val emp = spark.read.parquet("adl://demotechwithviresh.azuredatalakestore.net/Emp")
    emp.show()
  }

}