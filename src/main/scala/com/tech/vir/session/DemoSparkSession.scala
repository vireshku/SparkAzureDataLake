package com.tech.vir.session

import org.apache.spark.sql.SparkSession

object DemoSparkSession {

  def sparkSession(): SparkSession = {

    val spark = SparkSession.builder().appName("DemoTraining").master("local").getOrCreate()

    spark.conf.set("fs.adl.oauth2.access.token.provider.type", "ClientCredential")
    spark.conf.set("fs.adl.oauth2.client.id", "9f85d142-0304-4a91-832d-23e35cae4619")
    spark.conf.set("fs.adl.oauth2.credential", "nH7W7acU..0urK_2_PTt_48HL~V1Cpn7-B")
    spark.conf.set("fs.adl.oauth2.refresh.url", 
        "https://login.microsoftonline.com/c0bd411e-8c2c-42bd-887a-c89e0ba45d47/oauth2/token")

    spark
  }

}