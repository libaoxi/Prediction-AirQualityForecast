package org.template.vanilla

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.storage.Event
import io.prediction.data.storage.Storage

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import grizzled.slf4j.Logger

case class DataSourceParams(val appId: Int) extends Params

class DataSource(val dsp: DataSourceParams)
extends PDataSource[TrainingData,
EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {
    val eventsDb = Storage.getPEvents()
    // require(true,eventsDb)
    //Read all events involving "point" type 
    println("Gathering data from the event server")


    val training_points: RDD[LabeledPoint] = eventsDb.aggregateProperties(

      appId = dsp.appId,
      entityType = "training_point",

      // only keep entities with these required properties defined
      required = Some(List(
        "plan",
        "attr0",
        "attr1",
        "attr2", 
        "attr3",
        "attr4",
        "attr5",
        "attr7", 
        "attr8", 
        "attr9", 
        "attr10", 
        "attr11", 
        "attr12", 
        "attr13", 
        "attr14", 
        "attr15", 
        "attr16", 
        "attr17", 
        "attr18", 
        "attr19", 
        "attr20", 
        "attr21", 
        "attr22", 
        "attr23", 
        "attr24", 
        "attr25", 
        "attr26", 
        "attr27" 
        // "attr28" 
      )))(sc)
    // aggregateProperties() returns RDD pair of
    // entity ID and its aggregated properties
      .map { case (entityId, properties) =>
        try {
          //Converting to Labeled Point as the LinearRegression Algorithm requires
          LabeledPoint(properties.get[Double]("plan"),
            Vectors.dense(Array(
              properties.get[Double]("attr0"),
              properties.get[Double]("attr1"),
              properties.get[Double]("attr2"),
              properties.get[Double]("attr3"),
              properties.get[Double]("attr4"),	
              properties.get[Double]("attr5"),
              properties.get[Double]("attr6"),
              properties.get[Double]("attr7"),
              properties.get[Double]("attr8"),
              properties.get[Double]("attr9"),
              properties.get[Double]("attr10"),
              properties.get[Double]("attr11"),
              properties.get[Double]("attr12"),
              properties.get[Double]("attr13"),
              properties.get[Double]("attr14"),
              properties.get[Double]("attr15"),
              properties.get[Double]("attr16"),
              properties.get[Double]("attr17"),
              properties.get[Double]("attr18"),
              properties.get[Double]("attr19"),
              properties.get[Double]("attr20"),
              properties.get[Double]("attr21"),
              properties.get[Double]("attr22"),
              properties.get[Double]("attr23"),
              properties.get[Double]("attr24"),
              properties.get[Double]("attr25"),
              properties.get[Double]("attr26"),
              properties.get[Double]("attr27")
              // properties.get[Double]("attr28")
            ))
        )
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      }

      new TrainingData(training_points)
  }
}

class TrainingData(
  val training_points: RDD[LabeledPoint]
) extends Serializable
