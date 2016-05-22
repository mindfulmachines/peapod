package peapod

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.google.common.io.Resources
import org.apache.commons.codec.binary.Base64
import org.apache.commons.codec.net.URLCodec
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, StopWordsRemover, Tokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time.LocalDate
import org.scalatest.FunSuite
import StorableTask._
import Pea._
import peapod.PeapodTest.{AUC, Parsed, PipelineFeature, Raw}

object PeapodTest {
  var runs = 0

  def upRuns() = this.synchronized {
    runs+=1
  }

  case class DependencyInput(label: Double, text: String)

  @GenVersion
  class Raw(implicit val p: Peapod) extends StorableTask[RDD[DependencyInput]]
     {
    override val description = "Loading data from dependency.csv"
    def generate = {
      upRuns()
      p.sc.textFile("file://" + Resources.getResource("dependency.csv").getPath)
        .map(_.split(","))
        .map(l => new DependencyInput(l(0).toDouble, l(1)))
    }
  }

  @GenVersion
  class Parsed(implicit val p: Peapod) extends StorableTask[DataFrame] {
    import p.sqlCtx.implicits._
    val raw = pea(new Raw)
    def generate = {
      upRuns()
      raw().toDF()
    }
  }

  @GenVersion
  class ParsedEphemeral(implicit val p: Peapod) extends EphemeralTask[DataFrame] {
    import p.sqlCtx.implicits._
    val raw = pea(new Raw)
    def generate = {
      upRuns()
      raw().toDF()
    }
  }

  @GenVersion
  class PipelineFeature(implicit val p: Peapod) extends StorableTask[PipelineModel] {
    val parsed = pea(new Parsed)
    def generate = {
      upRuns()
      val training = parsed.get()
      val tokenizer = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("TextTokenRaw")
      val remover = new (StopWordsRemover)
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("TextToken")
      val hashingTF = new HashingTF()
        .setNumFeatures(5)
        .setInputCol(remover.getOutputCol)
        .setOutputCol("features")

      val pipeline = new org.apache.spark.ml.Pipeline()
        .setStages(Array(tokenizer,remover, hashingTF))
      pipeline.fit(training)
    }
  }

  @GenVersion
  class PipelineLR(implicit val p: Peapod) extends StorableTask[PipelineModel] {
    val pipelineFeature = pea(new PipelineFeature())
    val parsed = pea(new Parsed)
    def generate = {
      upRuns()
      val training = parsed
      val lr = new LogisticRegression()
        .setMaxIter(25)
        .setRegParam(0.01)
        .setFeaturesCol("features")
        .setLabelCol("label")
      val pipeline = new org.apache.spark.ml.Pipeline()
        .setStages(Array(lr))
      pipeline.fit(pipelineFeature.get().transform(training.get()))
    }
  }

  @GenVersion
  class AUC(implicit val p: Peapod) extends StorableTask[Double]  {
    override val description = "AUC generated for a model"
    val pipelineLR = pea(new PipelineLR())
    val pipelineFeature = pea(new PipelineFeature())
    val parsed = pea(new Parsed)
    def generate = {
      upRuns()
      val training = parsed.get()
      val transformed = pipelineFeature.get().transform(training)
      val predictions = pipelineLR.get().transform(transformed)
      val evaluator = new BinaryClassificationEvaluator()
      evaluator.evaluate(predictions)
    }
  }

}

class PeapodTest extends FunSuite {
  test("testMetaData") {
    val sdf = new SimpleDateFormat("ddMMyy-hhmmss")
    val path = System.getProperty("java.io.tmpdir") + "workflow-" + sdf.format(new Date())
    new File(path).mkdir()
    new File(path).deleteOnExit()
    implicit val w = new Peapod(
      path="file://" + path,
      raw="")(generic.Spark.sc)

    val pipelineVers = new PipelineFeature().version
    val parsedVers = new Parsed().version
    val rawVers = new Raw().version
    val aucVers = new AUC().version

    assert(new PeapodTest.AUC().metadata() ==
      s"peapod.PeapodTest$$AUC:$aucVers\n" +
        s"AUC generated for a model\n" +
        s"-peapod.PeapodTest$$PipelineFeature:$pipelineVers\n" +
        s"-peapod.PeapodTest$$Parsed:$parsedVers\n" +
        s"-peapod.PeapodTest$$Raw:$rawVers\n" +
        s"--Loading data from dependency.csv"
    )
  }
  test("testRunWorkflowConcurrentCache") {
    val sdf = new SimpleDateFormat("ddMMyy-hhmmss")
    val path = System.getProperty("java.io.tmpdir") + "workflow-" + sdf.format(new Date())
    new File(path).mkdir()
    new File(path).deleteOnExit()
    implicit val w = new Peapod(
      path="file://" + path,
      raw="")(generic.Spark.sc)


    PeapodTest.runs = 0
    val pea = w.pea(new PeapodTest.AUC())
    (1 to 10).par.foreach{i => w.pea(new PeapodTest.AUC()).get()}
    assert(PeapodTest.runs == 5)
    Thread.sleep(1000)
    PeapodTest.runs = 0
    (1 to 10).par.foreach{i => w.pea(new PeapodTest.AUC()).get()}
    assert(PeapodTest.runs == 0)
  }
  test("testRunWorkflow") {
    val sdf = new SimpleDateFormat("ddMMyy-hhmmss")
    val path = System.getProperty("java.io.tmpdir") + "workflow-" + sdf.format(new Date())
    new File(path).mkdir()
    new File(path).deleteOnExit()
    implicit val w = new Peapod(
      path="file://" + path,
      raw="")(generic.Spark.sc)

    PeapodTest.runs = 0
    w(new PeapodTest.PipelineFeature()).get()
    w(new PeapodTest.ParsedEphemeral())
    w(new PeapodTest.AUC())
    println(w.dotFormatDiagram())
    println(Util.gravizoDotLink(w.dotFormatDiagram()))
    println(Util.teachingmachinesDotLink(w.dotFormatDiagram()))

    println(w.pea(new PeapodTest.AUC()).get())
    assert(PeapodTest.runs == 5)
    PeapodTest.runs = 0
    println(w.pea(new PeapodTest.AUC()).get())
    assert(PeapodTest.runs == 0)
    PeapodTest.runs = 0
    println(w.pea(new PeapodTest.ParsedEphemeral()).get())
    assert(PeapodTest.runs == 1)
    println("http://g.gravizo.com/g?" +
      new URLCodec().encode(w.dotFormatDiagram()).replace("+","%20"))

  }

  test("testRunWorkflowConcurrent") {
    val sdf = new SimpleDateFormat("ddMMyy-hhmmss")
    val path = System.getProperty("java.io.tmpdir") + "workflow-" + sdf.format(new Date())
    new File(path).mkdir()
    new File(path).deleteOnExit()
    implicit val w = new Peapod(
      path="file://" + path,
      raw="")(generic.Spark.sc)

    PeapodTest.runs = 0
    (1 to 10).par.foreach{i => w.pea(new PeapodTest.AUC()).get()}
    assert(PeapodTest.runs == 5)
  }


}
