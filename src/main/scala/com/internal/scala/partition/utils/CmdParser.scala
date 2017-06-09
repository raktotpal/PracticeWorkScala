package com.internal.scala.partition.utils

import org.apache.commons.cli.Options
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.BasicParser
import org.apache.commons.cli.ParseException

class CmdParser {
    def parseCmdLine(args: Array[String], options: Options): CommandLine = {
        var cmd: CommandLine = null;
        try {
            val parser = new BasicParser();
            cmd = parser.parse(options, args)
        } catch {
            case e: ParseException => println("Invalid Command Line. " + e.getMessage())
            // e.printStackTrace();
        }
        return cmd;
    }
}