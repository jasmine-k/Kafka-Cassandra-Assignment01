package edu.knoldus

import twitter4j._
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class TwitterApp {

  val twitter: Twitter = TwitterFactory.getSingleton

  def getUserStatuses(userName: String): Future[List[String]] = Future{
    twitter.getUserTimeline(userName).asScala.toList.flatMap{
      tweet => tweet.getHashtagEntities.toList.map(_.getText)
    }
  }

}
