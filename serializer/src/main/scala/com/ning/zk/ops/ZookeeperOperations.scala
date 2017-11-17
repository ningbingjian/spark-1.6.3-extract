package com.ning.zk.ops

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode

/**
  *
  * Created by zhaoshufen
  * User:  zhaoshufen
  * Date: 2017/10/15
  * Time: 21:41
  * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
  */
case class Data(val name:String,val age:Int) extends Serializable
object ZookeeperOperations{
  private val ZK_CONNECTION_TIMEOUT_MILLIS = 15000
  private val ZK_SESSION_TIMEOUT_MILLIS = 60000
  private val RETRY_WAIT_MILLIS = 5000
  private val MAX_RECONNECT_ATTEMPTS = 3
  def main(args: Array[String]): Unit = {
   // putData()
    getData()
  }

  def putData()={
    val zk = newClient()
    val data = Data("u1",100)
    val bos = new ByteArrayOutputStream();
    val objOut = new ObjectOutputStream(bos);
    objOut.writeObject(data)
    val path = "/zktest/data"
    //zk.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path)
    zk.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path,bos.toByteArray)
    objOut.close()
  }
  def getData()={
    val zk = newClient()
    val path = "/zktest/data"
    //zk.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path)
    val data = zk.getData.forPath(path)
    val bos = new ByteArrayInputStream(data)
    val objInput = new ObjectInputStream(bos)
    val pojo = objInput.readObject().asInstanceOf[Data]
    println(pojo.name)

    objInput.close()
  }
  def newClient()={
    val zk = CuratorFrameworkFactory.newClient("localhost:2181",
      ZK_SESSION_TIMEOUT_MILLIS, ZK_CONNECTION_TIMEOUT_MILLIS,
      new ExponentialBackoffRetry(RETRY_WAIT_MILLIS, MAX_RECONNECT_ATTEMPTS))
    zk.start()
    zk
  }
}
class ZookeeperOperations {

}
