package peapod

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, StopWordsRemover, Tokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite
import StorableTask._

object PeapodWorkflowTest {
  var runs = 0

  def upRuns() = this.synchronized {
    runs+=1
  }

  case class DependencyInput(label: Double, text: String)

  class Raw(implicit val p: Peapod) extends StorableTask[RDD[DependencyInput]]
     {
    override val version = "2"
    override val description = "Loading data from dependency.csv"
    def generate = {
      upRuns()
      p.sc.textFile(p.raw + "/dependency.csv")
        .map(_.split(","))
        .map(l => new DependencyInput(l(0).toDouble, l(1)))
    }
  }

  class Parsed(implicit val p: Peapod) extends StorableTask[DataFrame] {
    import p.sqlCtx.implicits._
    val raw = pea(new Raw)
    def generate = {
      upRuns()
      raw().toDF()
    }
  }

  class ParsedEphemeral(implicit val p: Peapod) extends EphemeralTask[DataFrame] {
    import p.sqlCtx.implicits._
    val raw = pea(new Raw)
    def generate = {
      upRuns()
      raw().toDF()
    }
  }

  class PipelineFeature(implicit val p: Peapod) extends StorableTask[PipelineModel] {
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

abstract class PeapodWorkflowTest extends FunSuite {
  def generatePeapod(): Peapod

  test("testRunWorkflowConcurrentCache") {
    implicit val w = generatePeapod()
    PeapodWorkflowTest.runs = 0
    (1 to 10).par.foreach{i => w.pea(new PeapodWorkflowTest.AUC()).get()}
    assert(PeapodWorkflowTest.runs == 5)
    Thread.sleep(1000)
    PeapodWorkflowTest.runs = 0
    (1 to 10).par.foreach{i => w.pea(new PeapodWorkflowTest.AUC()).get()}
    assert(PeapodWorkflowTest.runs == 0)
  }
  test("testRunWorkflow") {
    implicit val w = generatePeapod()
    PeapodWorkflowTest.runs = 0
    w(new PeapodWorkflowTest.PipelineFeature()).get()
    w(new PeapodWorkflowTest.ParsedEphemeral())
    w(new PeapodWorkflowTest.AUC())

    w.pea(new PeapodWorkflowTest.AUC()).get()

    assert(PeapodWorkflowTest.runs == 5)

    PeapodWorkflowTest.runs = 0
    w.pea(new PeapodWorkflowTest.AUC()).get()

    assert(PeapodWorkflowTest.runs == 0)

    PeapodWorkflowTest.runs = 0

    w.pea(new PeapodWorkflowTest.ParsedEphemeral()).get()

    assert(PeapodWorkflowTest.runs == 1)
    assert(w.size() == 6)

  }

  test("testRunWorkflowConcurrent") {
    implicit val w = generatePeapod()
    PeapodWorkflowTest.runs = 0
    (1 to 10).par.foreach{i => w.pea(new PeapodWorkflowTest.AUC()).get()}
    assert(PeapodWorkflowTest.runs == 5)
  }


}
