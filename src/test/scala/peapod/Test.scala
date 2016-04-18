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

object Test {
  var runs = 0

  def upRuns() = this.synchronized {
    runs+=1
  }

  case class DependencyInput(label: Double, text: String)

  class Raw extends StorableTask[RDD[DependencyInput]] {
    override val version = "2"
    def generate(p: Peapod) = {
      upRuns()
      p.sc.textFile("file://" + Resources.getResource("dependency.csv").getPath)
        .map(_.split(","))
        .map(l => new DependencyInput(l(0).toDouble, l(1)))
    }
  }

  class Parsed extends StorableTask[DataFrame] {
    val raw = pea(new Raw)
    def generate(p: Peapod) = {
      import p.sqlCtx.implicits._
      upRuns()
      raw(p).get().toDF()
    }
  }

  class ParsedEphemeral extends EphemeralTask[DataFrame] {

    val raw = pea(new Raw)
    def generate(p: Peapod) = {
      import p.sqlCtx.implicits._
      upRuns()
      raw(p).get().toDF()
    }
  }

  class PipelineFeature extends StorableTask[PipelineModel] {
    val parsed = pea(new Parsed)
    override val version = "2"
    def generate(p: Peapod) = {
      upRuns()
      val training = parsed(p).get()
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

  class PipelineLR extends StorableTask[PipelineModel] {
    val pipelineFeature = pea(new PipelineFeature)
    val parsed = pea(new Parsed)
    def generate(p: Peapod) = {
      upRuns()
      val training = parsed(p).get()
      val lr = new LogisticRegression()
        .setMaxIter(25)
        .setRegParam(0.01)
        .setFeaturesCol("features")
        .setLabelCol("label")
      val pipeline = new org.apache.spark.ml.Pipeline()
        .setStages(Array(lr))
      pipeline.fit(pipelineFeature(p).get().transform(training))
    }
  }

  class AUC extends StorableTask[Double] {
    val pipelineLR = pea(new PipelineLR)
    val pipelineFeature = pea(new PipelineFeature)
    val parsed = pea(new Parsed)
    def generate(p: Peapod) = {
      upRuns()
      val training = parsed(p).get()
      val transformed = pipelineFeature(p).get().transform(training)
      val predictions = pipelineLR(p).get().transform(transformed)
      val evaluator = new BinaryClassificationEvaluator()
      evaluator.evaluate(predictions)
    }
  }

  class AUCPartitioned(val partition: LocalDate)
    extends StorableTask[Double] with PartitionedTask[Double] {
    val pipelineLR = pea(new PipelineLR)
    val pipelineFeature = pea(new PipelineFeature)
    val parsed = pea(new Parsed)
    def generate(p: Peapod) = {
      upRuns()
      val training = parsed(p).get()
      val transformed = pipelineFeature(p).get().transform(training)
      val predictions = pipelineLR(p).get().transform(transformed)
      val evaluator = new BinaryClassificationEvaluator()
      evaluator.evaluate(predictions)
    }
  }
}

class Test extends FunSuite {
  test("testRunWorkflow") {
    implicit val sc = generic.Spark.sc
    val sdf = new SimpleDateFormat("ddMMyy-hhmmss")
    val path = System.getProperty("java.io.tmpdir") + "workflow-" + sdf.format(new Date())
    new File(path).mkdir()
    new File(path).deleteOnExit()
    implicit val w = new Peapod(
      path="file://" + path,
      raw="")

    w.pea(new Test.PipelineFeature).get()
    w.pea(new Test.ParsedEphemeral)
    w.pea(new Test.AUC)
    println(w.dotFormatDiagram())
    println(Util.gravizoDotLink(w.dotFormatDiagram()))
    println(Util.teachingmachinesDotLink(w.dotFormatDiagram()))

    println(w.pea(new Test.AUC).get())
    assert(Test.runs == 5)
    Test.runs = 0
    println(w.pea(new Test.AUC).get())
    assert(Test.runs == 0)
    Test.runs = 0
    println(w.pea(new Test.ParsedEphemeral).get())
    assert(Test.runs == 1)
    println("http://g.gravizo.com/g?" +
      new URLCodec().encode(w.dotFormatDiagram()).replace("+","%20"))



    println(w.pea(new Test.AUCPartitioned(new LocalDate)).get())
  }
}
