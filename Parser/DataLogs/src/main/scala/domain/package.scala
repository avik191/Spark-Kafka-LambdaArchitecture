package object domain {

  case class Activity(
                     timestamp_hour : Long,
                     referrer : String,
                     action : String,
                     prevPage : String,
                     page : String,
                     visitor : String,
                     product : String,
                     inputProps : Map[String,String] = Map()
                     )
  case class Logs(
                       timestamp_hour : Long,
                       referrer : String,
                       action : String,
                       prevPage : String,
                       page : String,
                       visitor : String,
                       product : String

                     )

  case class ActivityLog (
                           product : String,
                           timestamp_hour : Long,
                           purchase : Long,
                           add_to_cart : Long,
                           page_view : Long
                         )

  case class VisitorsByProduct (product : String, timestamp_hour : Long, unique_visitors : Long)

}
