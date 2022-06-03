package com.kumargaurav

import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}
import java.util.{Date, Locale, TimeZone}

object DateTimeTest {
  def main(args: Array[String]): Unit = {
    println("localdate -> " + LocalDate.now)
    val expirationWindowDate: Date = convertToDateViaInstant(LocalDate.now.minusDays(90))
    println("90 days back date -> " + expirationWindowDate)
    val expirationWindowDateStr: String = getDateAsString(expirationWindowDate)
    println("90 days back date str -> " + expirationWindowDateStr)
    if (expirationWindowDate.getTime >= expirationWindowDate.getTime) {
      println("success")
    }
  }

  private def convertToDateViaInstant(dateToConvert: LocalDate) = java.util.Date.from(dateToConvert.atStartOfDay.atZone(ZoneId.systemDefault).toInstant)

  def getDateAsString(d: Date): String = {
    val DATE_FORMAT = "yyyy-MM-dd hh:mm:ss.SSS"
    val dateFormat = new SimpleDateFormat(DATE_FORMAT, Locale.US)
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    dateFormat.format(d)
  }
}
