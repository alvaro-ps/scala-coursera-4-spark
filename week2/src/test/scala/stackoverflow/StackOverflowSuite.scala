package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.junit._
import org.junit.Assert.assertEquals
import java.io.File

object StackOverflowSuite {
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  val sc: SparkContext = new SparkContext(conf)
}

class StackOverflowSuite {
  import StackOverflowSuite._


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  @Test def `testObject can be instantiated`: Unit = {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  @Test def `test groupPostings`: Unit = {
    import StackOverflow.groupedPostings

    val raw = sc.parallelize(List(
      Posting(1, 1, None, None, 0, Some("Scala")),
      Posting(2, 2, None, Some(1), 2, Some("Scala")),
      Posting(1, 3, None, None, 0, Some("Python")),
      Posting(2, 4, None, Some(3), 5, Some("Python")),
      Posting(1, 5, None, None, 0, Some("Haskell")),
    ))

    val expected = List(
      (
        1, 
        List(
          (
            Posting(1, 1, None, None, 0, Some("Scala")),
            Posting(2, 2, None, Some(1), 2, Some("Scala"))
          )
        )
      ),
      (
        3, 
        List(
          (
            Posting(1, 3, None, None, 0, Some("Python")),
            Posting(2, 4, None, Some(3), 5, Some("Python")),
          )
        )
      )
    )
    val group = groupedPostings(raw).collect.toList
    assert(group == expected, s"$group should be $expected")
  }

  @Test def `test scoredPostings`: Unit = {
    import StackOverflow.scoredPostings

    val grouped = sc.parallelize(List[(QID, Iterable[(Question, Answer)])](
      (
        1, 
        List(
          (
            Posting(1, 1, None, None, 0, Some("Scala")),
            Posting(2, 2, None, Some(1), 2, Some("Scala"))
          )
        )
      ),
      (
        3, 
        List(
          (
            Posting(1, 3, None, None, 0, Some("Python")),
            Posting(2, 4, None, Some(3), 5, Some("Python")),
          )
        )
      )
    ))
    val expected = List(
      (Posting(1, 1, None, None, 0, Some("Scala")), 2),
      (Posting(1, 3, None, None, 0, Some("Python")), 5)
    )

    val scores = scoredPostings(grouped).collect.toList
    assert(scores == expected, s"$scores should be $expected")
  }

  @Test def `test vectorPostings`: Unit = {
    import StackOverflow.{vectorPostings, langs, langSpread}

    val scored = sc.parallelize(List(
      (Posting(1, 1, None, None, 0, Some("Scala")), 2),
      (Posting(1, 3, None, None, 0, Some("Python")), 5)
    ))
    val expected = List(
      (langs.indexOf("Scala")*langSpread, 2),
      (langs.indexOf("Python")*langSpread, 5)
    )

    val vectors = vectorPostings(scored).collect.toList
    assert(vectors == expected, s"$vectors should be $expected")
  }

  @Rule def individualTestTimeout = new org.junit.rules.Timeout(100 * 1000)
}
