package com.tech.vir.entry

import com.tech.vir.session.DemoSparkSession
import scala.reflect.internal.Mode
import org.apache.spark.sql.SaveMode

object MainEntry {
  
  def main(args: Array[String]): Unit = {
    
    
    val spark = DemoSparkSession.sparkSession()
    
    import spark.implicits._
    
    // Employee Data
    
    val empDF = Seq((10, "John" , 100),(11, "Garner" , 200),(12, "Mike" , 300)).toDF("EmpId" , "EmpName" , "DepId")
    empDF.show()
     
    // Department Data
    
    val depDf = Seq((100, "IT"),(200, "Accounts"),(300, "HR")).toDF("DepId","DepName")
    depDf.show()
    
    // Resultant data
    
    val resultant = empDF.join(depDf, "DepId").select($"EmpName", $"DepName")
    
    resultant.show()
    
    println("Finsihn------------->")
    
  }
  
  
}