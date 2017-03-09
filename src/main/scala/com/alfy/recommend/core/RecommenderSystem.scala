package com.alfy.recommend.core

import java.io.Serializable

import akka.actor.{Props, Actor, ActorLogging}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

/**
  * Companion object for the RecommenderSystem
  */
object RecommenderSystem {
  case object Train {}
  case class GenerateRecommendations(userId: Int)
  case class Recommendation(contentItemId: Int, rating: Double)
  case class Recommendations(items: Seq[Recommendation])

  /**
    * Defines the properties for the recommender system actor
    * @param sc Spark context to use
    * @return   Returns the properties for the actor
    */
  def props(sc: SparkContext) = Props(new RecommenderSystem(sc))
}

/**
  * Implements a trainable recommender system
  * @param sc Spark context to use
  */
class RecommenderSystem(sc: SparkContext) extends Actor with ActorLogging {

  import RecommenderSystem._

  var model: Option[MatrixFactorizationModel] = None

  def receive = {
    case Train => trainModel()
    case GenerateRecommendations(userId) => generateRecommendations(userId, 10)
    case ModelTrainer.TrainingResult(model) => storeModel(model)
  }

  /**
    * Trains a new version of the recommender system model
    * @return Returns the trained model
    */
  private def trainModel() = {
    // Start a separate actor to train the recommendation system.
    // This enables the service to continue service requests while it learns new recommendations.
    val trainer = context.actorOf(ModelTrainer.props(sc), "model-trainer")
    trainer ! ModelTrainer.Train
  }

  private def storeModel(model: MatrixFactorizationModel) = {
    this.model = Some(model)
  }

  /**
    * Generates recommendations
    * @param userId User ID for which to generate recommendations
    */
  private def generateRecommendations(userId: Int, count: Int) = {
    log.info(s"Generating ${count} recommendations for user with ID ${userId}")

    // Generate recommendations based on the machine learning model.
    // When there's no trained model return an empty list instead.
    val results = model match {
      case Some(m) => m.recommendProducts(userId,count)
        .map(rating => Recommendation(rating.product,rating.rating))
        .toList

      case None => Nil
    }

    sender ! Recommendations(results)
  }
}
