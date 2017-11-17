package com.ning.ha.scala

import java.util.concurrent.{Executors, TimeUnit}

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.SparkCuratorUtil
import org.apache.spark.deploy.master.{LeaderElectable, ZooKeeperLeaderElectionAgent}


/**
  *
  * Created by zhaoshufen
  * User:  zhaoshufen
  * Date: 2017/10/14
  * Time: 21:53
  * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
  */
trait LeaderElectable {
  def electedLeader()
  def notLeader()
  def revokedLeadership()
}
object NingMaster{
  //负责等待外部触发进程结束
  private val pool = Executors.newFixedThreadPool(1)
  def main(args: Array[String]): Unit = {
    new SparkContext().textFile("").flatMap(line =>line.split(""))
      .map(w => (w,1)).reduceByKey(_ + _)
    new NingMaster(args(0))
    pool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
  }
}
class NingMaster(name:String) extends LeaderElectable{

  println(s"master start with name ${name}")
  private val leaderElectionAgent_ = new ZooKeeperLeaderElectionAgent(this)
  override def notLeader() = {
    println(s"${name} is not leader now")
  }
  override def electedLeader() = {
    println(s"${name} is leader now")
  }

  override def revokedLeadership() = {
      println(s"${name} Leadership has been revoked -- master shutting down.")
      System.exit(0)
  }



}
class ZooKeeperLeaderElectionAgent(val masterInstance: LeaderElectable) extends  LeaderLatchListener with LeaderElectionAgent{
  val WORKING_DIR = "/ning/ha/leader_election"
  private var zk: CuratorFramework = _
  private var leaderLatch: LeaderLatch = _
  private var status = LeadershipStatus.NOT_LEADER

  private val ZK_CONNECTION_TIMEOUT_MILLIS = 15000
  private val ZK_SESSION_TIMEOUT_MILLIS = 60000
  private val RETRY_WAIT_MILLIS = 5000
  private val MAX_RECONNECT_ATTEMPTS = 3

  start()

  def newClient(zkUrl:String): CuratorFramework = {
    val ZK_URL = zkUrl
    val zk = CuratorFrameworkFactory.newClient(ZK_URL,
      ZK_SESSION_TIMEOUT_MILLIS, ZK_CONNECTION_TIMEOUT_MILLIS,
      new ExponentialBackoffRetry(RETRY_WAIT_MILLIS, MAX_RECONNECT_ATTEMPTS))
    zk.start()
    zk
  }

  def start() {
    zk = newClient("localhost:2181")
    leaderLatch = new LeaderLatch(zk, WORKING_DIR)
    leaderLatch.addListener(this)
    leaderLatch.start()
  }
  override def isLeader = {
    synchronized {
      masterInstance.electedLeader()
    }
  }
  override def notLeader() = {
    synchronized{
      masterInstance.notLeader()
    }
  }

  override def stop(): Unit = {
    leaderLatch.close()
    zk.close()
  }

}
object LeadershipStatus extends Enumeration {
  type LeadershipStatus = Value
  val LEADER, NOT_LEADER = Value
}

trait LeaderElectionAgent {
  val masterInstance: LeaderElectable
  def stop() {} // to avoid noops in implementations.
}
