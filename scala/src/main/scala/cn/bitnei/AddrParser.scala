package cn.bitnei

import addrparser.MyHandler
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import tech.spiro.addrparser.io.file.JSONFileRegionDataInput
import tech.spiro.addrparser.io.file.JSONFileRegionDataInput
import tech.spiro.addrparser.parser.{Location, LocationParserEngine}

import java.io.File

/**
 * @author ngt on 2021-06-08 18:23
 * @version 1.0
 */
object AddrParser {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val lines: DataStream[String] = env.readTextFile("data/alarm.csv")

    var BEVList = List(
      "BJ6425EVAA2", "BJ6480LABEV", "BJ6480LABEV1", "BJ6480LABEV2", "BJ6480LABEV3",
      "CSA6461FBEV1", "CSA6461FBEV2", "GAM6480BEVB0B", "GAM6480BEVB0C", "GAM6480BEVB0E",
      "CSA6461FBEV1", "GTM6450GBEV", "SZS6460A05BEV", "SZS6460A06BEV", "SZS6460A07BEV",
      "SZS6460A20BEV", "SZS6460A21BEV", "SZS6460A30BEV", "SZS6460A38BEV", "SZS6480A01BEV",
      "SZS6480A03BEV", "BJ7000C5D2-BEV", "BJ7000C5D3-BEV", "BJ7000C5D3K-BEV", "BJ7000C5D4-BEV",
      "BJ7000C5D5-BEV", "BJ7000C5D7-BEV", "BJ7000C5E6-BEV", "BJ7000C5E7-BEV", "BJ7000C5E7G-BEV",
      "BJ7000C5E8-BEV", "BJ7000C5E9-BEV", "BJ7000C5EC-BEV", "BJ7000C5ED-BEV", "BJ7000CTD-BEV",
      "BJ7001U5E2-BEV", "CC7000CE04BEV", "DFL7000NA81BEV", "DFL7000NAH1BEV", "DFL7000NAH2BEV",
      "DFM7000G1F3PBEV", "DFM7000G1F6BEV", "DFM7000G1F6PBEV", "DFM7000G1F7BEV", "FV7002AABEV",
      "FV7002BABEV", "GAM7000BEVA0C", "GAM7000BEVA0E", "GAM7000BEVA0F", "GTM70002BEV", "HQ7002BEV04",
      "HQ7002BEV05", "HQ7002BEV08", "HQ7002BEV11", "HQ7002BEV12", "HQ7002BEV15", "HQ7002BEV17",
      "HQ7002BEV35", "HQ7002BEV36", "HQ7002BEV37", "HQ7002BEV45", "HQ7002BEV56", "HQ7002BEV57",
      "HQ7002BEV66", "JHC7002BEV27", "JHC7002BEV33", "JHC7002BEV34", "JHC7002BEV38", "JHC7002BEV41",
      "JHC7002BEV46", "JHC7002BEV51", "JHC7002BEV63", "JHC7003BEV01", "JHC7003BEV06", "MR7002BEV03",
      "MR7002BEV08", "MR7002BEV11", "MR7002BEV22", "MR7002BEV23", "SQR7000BEVJ601", "SQR7000BEVJ604",
      "SQR7000BEVJ605", "CSA6456BEV2"
    )

    val PHEVList = List(
      "CC6481AD23CPHEV", "CC6483AD21APHEV", "CC6483AD22FPHEV", "CC6483AD23CPHEV", "CC6484AD21APHEV",
      "CSA6454NEPHEV2", "CSA6463NEPHEV", "CSA6468NEPHEV", "FV6462LCDCHEV", "GAC6450CHEVA5C",
      "GAC6450CHEVA5E", "GAC6451CHEVA6", "GFA6490CHEV1DA", "SH6471P1PHEV", "SH6484N1PHEV",
      "SVW6471APV", "SVW6472APV", "BH7160PHEVCBAS", "BH7200PHEVRAS", "BH7200PHEVRAV",
      "BH7201PHEVRAS", "FV7147FADCHEV", "HQ7152PHEV02", "HQ7152PHEV05", "HQ7152PHEV10",
      "HQ7152PHEV16", "MR7152PHEV01", "SVW7141BPV", "SVW7142BPV", "YQZ7201PHEV", "YQZ7202GPHEV",
      "YQZ7202PHEV", "CSA6454NDPHEV1", "CSA6454NDPHEV2", "BMW6462AAHEV(BMWX1)", "BMW6462ABHEV(BMWX1)",
      "BMW6462ACHEV(BMWX1)", "BMW6462ADHEV(BMWX1)", "MR7153PHEV01", "MR7153PHEV05", "MR7153PHEV08",
      "MR7153PHEV18", "MR7153PHEV24")

    var powerType: String = ""
    val value: DataStream[VehicleOutput] = lines.filter(!_.contains("veh_model_name"))
      .map(data => {
        val arr: Array[String] = data.split(",")
        if (BEVList.contains(arr(1))) {
          powerType = "BEV"
        }
        if (PHEVList.contains(arr(1))) {
          powerType = "PHEV"
        }
        (powerType, arr(2), arr(12).toInt, arr(4), arr(22), arr(19), arr(20), arr(15).toDouble, arr(16).toDouble,System.currentTimeMillis())
      })
      .assignAscendingTimestamps(data => data._10)
      .map(data => {
        VehicleInput(data._1, data._3, data._4, data._5, data._6, data._7, data._8, data._9)
      }).map(data => {
      val location: Location = MyHandler.getPoint(data.lon / 1000000, data.lat / 1000000)
      data.province = location.getProv.getName
      data.city = location.getCity.getName
      data
    })
      .map(data => VehicleOutput(data.powerType, data.category, data.alarmName, data.pdate, data.province, data.city))
      .filter(data => data.category == 10 || data.category == 30)


    val filePath: String = "data/ts.csv"
    val file: File = new File(filePath)
    if (file.exists()) {
      file.delete()
    }

    value
      .writeAsCsv(filePath)

    env.execute()
  }

}

case class VehicleInput(
                         var powerType: String, // 2
                         category: Int, // 12
                         alarmName: String, // 4
                         var pdate: String, // 22
                         var province: String, // 19
                         var city: String, // 20
                         lon: Double,
                         lat: Double,
                       )

case class VehicleOutput(
                          var powerType: String, // 2
                          category: Int, // 12
                          alarmName: String, // 4
                          var pdate: String, // 22
                          var province: String, // 19
                          var city: String, // 20
                        )
