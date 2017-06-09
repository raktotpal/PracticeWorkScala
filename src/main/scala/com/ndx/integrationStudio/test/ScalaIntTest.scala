package com.internal.scala.Test

import java.text.SimpleDateFormat

object ScalaIntTest {
    def main(args: Array[String]) {
        val timeGMT = 1435689100078L;

        val formatTime = "yyyy";

        val sdf = new SimpleDateFormat(formatTime)

        println(sdf.format(timeGMT))
    }
}