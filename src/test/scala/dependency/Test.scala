package dependency

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.google.common.io.Resources
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, StopWordsRemover, Tokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

object Test {
  var runs = 0

  def upRuns() = this.synchronized {
    runs+=1
  }

  case class DependencyInput(label: Double, text: String)

  class Raw(implicit _p: Peapod) extends StorableTask[RDD[DependencyInput]] {
    override val version = "2"
    def generate = {
      upRuns()
      p.sc.textFile("file://" + Resources.getResource("dependency.csv").getPath)
        .map(_.split(","))
        .map(l => new DependencyInput(l(0).toDouble, l(1)))
    }
  }

  class Parsed(implicit _p: Peapod) extends EphemeralTask[DataFrame] {
    import p.sqlCtx.implicits._
    val raw = pea(new Raw)
    def generate = {
      upRuns()
      raw.get().df
    }
  }

  class PipelineFeature(implicit _p: Peapod) extends StorableTask[PipelineModel] {
    val parsed = pea(new Parsed)
    override val version = "2"
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

  class PipelineLR(implicit _p: Peapod) extends StorableTask[PipelineModel] {
    val pipelineFeature = pea(new PipelineFeature())
    val parsed = pea(new Parsed)
    def generate = {
      upRuns()
      val training = parsed.get()
      val lr = new LogisticRegression()
        .setMaxIter(25)
        .setRegParam(0.01)
        .setFeaturesCol("features")
        .setLabelCol("label")
      val pipeline = new org.apache.spark.ml.Pipeline()
        .setStages(Array(lr))
      pipeline.fit(pipelineFeature.get().transform(training))
    }
  }

  class AUC(implicit _p: Peapod) extends StorableTask[Double] {
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

class Test extends FunSuite {
  test("testRunWorkflow") {
    implicit val sc = generic.Spark.sc
    val sdf = new SimpleDateFormat("ddMMyy-hhmmss")
    val path = System.getProperty("java.io.tmpdir") + "workflow-" + sdf.format(new Date())
    new File(path).mkdir()
    new File(path).deleteOnExit()
    implicit val w = new Peapod(
      fs="file://",
      path=path,
      raw="")
    println(new Test.AUC().get())
    assert(Test.runs == 5)
    Test.runs = 0
    println(new Test.AUC().get())
    assert(Test.runs == 2)
    Test.runs = 0
    println(new Test.Parsed().get())
    assert(Test.runs == 1)
  }
}
