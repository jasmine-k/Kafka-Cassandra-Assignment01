package edu.knoldus

import com.datastax.driver.core.{Row, Session}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._

object CassandraInsert extends CassandraProvider{

  private val log: Logger = LoggerFactory.getLogger(getClass.getName)

  def insertHashTags(hashTag: String): List[Row] = {

    cassandraConn.execute(s"CREATE TABLE IF NOT EXISTS hashtagtable (dateandtime timestamp PRIMARY KEY,hashtag text) ")
    log.info(s"Inserting $hashTag")
    cassandraConn.execute(s"INSERT INTO hashtagtable(dateandtime,hashtag) VALUES (dateOf(now()),'$hashTag')").asScala.toList
  }

}
