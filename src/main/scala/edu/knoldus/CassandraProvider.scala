package edu.knoldus

import com.datastax.driver.core.{Cluster, Session}
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}

trait CassandraProvider {

  val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  val config: Config = ConfigFactory.load()
  val cassandraKeyspace: String = config.getString("cassandra.keyspace")
  val cassandraHostname: String = config.getString("cassandra.contact.points")

  val cassandraConn: Session = {
    val cluster = new Cluster.Builder().withClusterName("Test Cluster").
      addContactPoints(cassandraHostname).build
    val session = cluster.connect
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS  $cassandraKeyspace WITH REPLICATION = " +
      s"{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    session.execute(s"USE $cassandraKeyspace")
    session
  }

}
