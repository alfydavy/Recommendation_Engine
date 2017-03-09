package com.alfy.recommend

import akka.actor.ActorSystem
import akka.util.Timeout
import com.infosupport.recommendedcontent.restservice.RestService
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration._

/**
  * Entrypoint for the application
  */
object Program extends App {
  implicit val system = ActorSystem("recommended-content-service")
  implicit val timeout: Timeout = 30 seconds

  val service = new RestService("localhost")

  service.start()
}