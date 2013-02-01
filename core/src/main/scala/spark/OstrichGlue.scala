package spark

import com.twitter.conversions.time._
import com.twitter.ostrich.admin.AdminHttpService
import com.twitter.ostrich.admin.AdminServiceFactory
import com.twitter.ostrich.admin.JsonStatsLoggerFactory
import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.ostrich.admin.StatsFactory
import com.twitter.ostrich.stats.Stats

object OstrichGlue extends Logging {
  var httpService: AdminHttpService = null
  def start() {
    if (httpService == null) {
      var lock = new Object
      val threadGroup = new ThreadGroup("ostrich")
      threadGroup.setDaemon(true)

      val thread = new Thread(threadGroup, "ostrich server thread") {
        override def run = {
          lock.synchronized {
            val factory = AdminServiceFactory(
              httpPort = 0,
              statsNodes = List(StatsFactory(reporters=
                List(JsonStatsLoggerFactory(
                  period = 1.second
                ))
              ))
            )
            val environment = RuntimeEnvironment(this, Array[String]())
            httpService = factory(environment)
            lock.notifyAll
          }
        }
      }
      thread.start
      lock.synchronized {
        while (httpService == null) {
          lock.wait
        }
      }
    }
  }
}
