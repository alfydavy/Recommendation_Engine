package com.alfy.recommend.core

import akka.actor.{Props, ActorLogging, Actor}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{Rating, ALS, MatrixFactorizationModel}

import com.datastax.spark.connector._

/**
  * Companion object for the model trainer
  */
object ModelTrainer {

  /**
    * Message used to instruct the trainer to start the training process
    */
  case object Train

  /**
    * Message structure to deliver the results of the training process
    * @param model  Trained model
    */
  case class TrainingResult(model: MatrixFactorizationModel)

  /**
    * Defines the properties for the actor.
    * @param sc Spark context to use for training the recommender system
    * @return   Returns the the properties for the actor
    */
  def props(sc: SparkContext) = Props(new ModelTrainer(sc))
}

/**
  * Used to train a new version of the recommender system model
  * @param sc Spark context to use
  */
class ModelTrainer(sc: SparkContext) extends Actor with ActorLogging {

  import ModelTrainer._

  def receive = {
    case Train => trainModel()
  }

  /**
    * Trains the new recommender system model
    */
  private def trainModel() = {
    val table = context.system.settings.config.getString("cassandra.table")
    val keyspace = context.system.settings.config.getString("cassandra.keyspace")

    // Retrieve the ratings given by users from the database.
    // Map them to the rating structure needed by the Alternate Least Squares algorithm.
val ratings = sc.cassandraTable(keyspace, table).map(record => Rating(record.get[Int]("user_id"),
  record.get[Int]("item_id"), record.get[Double]("rating")))

// These settings control how well the predictions are going
// to fit the actual observations we loaded from Cassandra.
// Modify these to optimize the model!
val rank = 10
val iterations = 10
val lambda = 0.01

val model = ALS.train(ratings, rank, iterations, lambda)
    sender ! TrainingResult(model)

    context.stop(self)
  }
}
