package com.alfy.recommend.restservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.alfy.recommend.core.RecommenderSystem.{Recommendation, Recommendations}
import com.infosupport.recommendedcontent.core.RecommenderSystem
import spray.json.DefaultJsonProtocol

/**
  * Protocols for the service
  */
trait RestServiceProtocol extends DefaultJsonProtocol with SprayJsonSupport {
  implicit def recommendationFormat = jsonFormat2(Recommendation)
  implicit def recommendationsFormat = jsonFormat1(Recommendations)
  implicit def errorResponseFormat = jsonFormat1(ErrorResponse)
  implicit def genericResponseFormat = jsonFormat1(GenericResponse)
}
