package com.ning.jdbc.driver

import java.sql.DriverManager

/**
  *
  * Created by zhaoshufen
  * User:  zhaoshufen
  * Date: 2017/11/17
  * Time: 12:02
  * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
  */
object JdbcTest {
  def main(args: Array[String]): Unit = {
    //add driver
    //org.apache.hive.jdbc
    val driver="org.apache.hive.jdbc.HiveDriver"
    Class.forName(driver)

    //get connection
    val (url,username,userpasswd)=("jdbc:hive2://master:10003/default","hive","hive")
    val connection=DriverManager.getConnection(url,username,userpasswd)

    //get statement
    connection.prepareStatement("use tvlog").execute()
    val sql="select * from tvlog.tvlog_tcl2 limit 21"
    val statement=connection.prepareStatement(sql)

    //get result
    val rs=statement.executeQuery()
    while(rs.next()){
      println(s"${rs.getString(1)}:${rs.getString(2)}")
    }

    //close
    rs.close()
    statement.close()
    connection.close()
  }
}
