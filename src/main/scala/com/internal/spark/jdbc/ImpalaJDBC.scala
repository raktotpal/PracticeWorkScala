package com.internal.spark.jdbc

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

object ImpalaJDBC {

  //    val SQL_STATEMENT = "select sid from student limit 5"
  //    val IMPALAD_HOST = "dayrhectod010"
  //    val IMPALAD_JDBC_PORT = "21000";
  //    val CONNECTION_URL = "jdbc:hive2://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT// + "/;auth=noSasl";
  //    val JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

  def main(args: Array[String]) {
    val SQL_STATEMENT = "select sid from student limit 5"
    val IMPALAD_HOST = "XXXX"
    val IMPALAD_JDBC_PORT = "21050";
    val CONNECTION_URL = "jdbc:hive2://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/;auth=noSasl";
    val JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

    var con: Connection = null;

    Class.forName(JDBC_DRIVER_NAME).newInstance

    con = DriverManager.getConnection(CONNECTION_URL);

    val stmt = con.createStatement();

    val rs = stmt.executeQuery(SQL_STATEMENT);

    println("\n== Begin Query Results ======================");

    // print the results to the console
    while (rs.next()) {
      // the example query returns one String column
      println(rs.getString(1));
    }

    println("== End Query Results =======================\n\n");

  }
}