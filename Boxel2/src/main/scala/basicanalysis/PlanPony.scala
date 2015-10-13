/*
 * Final project for the class "Analyse von großen Datensaetzen in den Lebenswissenschaften"
 * (Freie Universität Berlin, SS15)
 *
 * Authors: Borbála Tasnádi & Axel Leonhardt (Team Boxel)
 *
 */

package basicanalysis

import java.io.File

import breeze.numerics._
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.preprocessing.StandardScaler
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.util.Collector

import scala.util.control.Exception._

object PlanPony {

  def main(args: Array[String]) {

    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    //*******************************************************************
    //        Preprocessing Clinical Data
    //*******************************************************************

    // read in and recode / normalize clinical data
    val clinDataSet = getClinDataSet(env).map(smokHisNorm(_))
                                          .filter(o => o.smokHis > -0.1 && (allCatch opt o.daysToBirth.toDouble).isDefined)
                                          .map(o => new ClinData2(o.patientId,
                                                                  genderMap(o.gender),
                                                                  tumSizeMap(o.tumSize),
                                                                  locMetMap(o.locMet),
                                                                  o.smokHis,
                                                                  -o.daysToBirth.toDouble / 36525,
                                                                  -o.daysToBirth.toDouble / 365.25 - o.ageAtDiag.toDouble))
                                                                  .filter(o => (o.locMet > -0.1) && (o.tumSize > -0.1))


    // read in the sample-file-map -> column1: sample TCGA-barcode
    //                                         -> column2: file name
    val sampleFileMap = getAliquots(env)

    // get a patient-file-map by shortening the sample barcode to the patient barcode
    // (note: the sample barcode starts with the according patient's barcode)
    val patFileMap = sampleFileMap.map(a => new Aliq(a.file, a.sampleID.substring(0, 12)))

    // in the clinical table, we substitute the patient ID with the file
    // associated to it by the patient-file-map
    val clinDataSet2 = clinDataSet.join(patFileMap).where(_.patientId).equalTo("sampleID")
                                  .apply((x, y) => new ClinData3(y.file,
                                                                x.gendInd,
                                                                x.tumSize,
                                                                x.locMet,
                                                                x.smokHis,
                                                                x.age,
                                                                x.tumAge))


    //*******************************************************************
    //        Reading Genomic Data
    //*******************************************************************

    // read in the genomic data one bye one adding a column containing the actual file name
    // and unite the obtained datasets  ->  column1: file name
    //                                      column2: gene Id
    //                                      column3: normalized count
    // (def of getGenoDataSet & getStartGenTable  below)
    val path = new File(genoPath)
    val listOfGenFiles = path.listFiles.filter(_.getName.endsWith("genes.normalized_results"))
    var bigGenTable = env.fromCollection[ModGenoData](Nil)

    for (i <- 0 to listOfGenFiles.length -1) {
      val genTable = getGenoDataSet(env, listOfGenFiles(i)).map(o => new ModGenoData(listOfGenFiles(i).getName,
                                                                          o.geneID.split('|')(1), o.normCount / 10000))
      bigGenTable = bigGenTable.union(genTable)
    }

    //*******************************************************************
    //        Merging Clinical and Genomic Data
    //*******************************************************************

    // join the stage-file-map and the united genomic data by file name
    val joined = bigGenTable.join(clinDataSet2).where("file").equalTo("file")
                                                      .apply((x, y) => new GeneFact(x.file,
                                                                                    x.geneID.toInt,
                                                                                    x.normCount,
                                                                                    y.gendInd,
                                                                                    y.tumSize,
                                                                                    y.locMet,
                                                                                    y.smokHis,
                                                                                    y.age,
                                                                                    y.tumAge))

                                                      // eliminate multiple rows
                                                      .groupBy("file", "geneID").reduceGroup(elim(_))


    //*******************************************************************
    //    Detection of Genes with Highest Correlation with Metastasis
    //*******************************************************************

    // find the 100 most correlated genes to metastasis
    // using Pearson correlation coefficient
    val pearsons = joined.map(o => (o.geneID, o.normCount, o.locMet))
                          .map(Thing => (Thing._1,Thing._2,Thing._3,Thing._2*Thing._3,Thing._2*Thing._2,Thing._3*Thing._3,1))
                          .groupBy(0).sum(1).andSum(2).andSum(3).andSum(4).andSum(5).andSum(6)
                          .map(FirstD=>(1,FirstD._1,(FirstD._7*FirstD._4-FirstD._2*FirstD._3)/(sqrt(Math.abs(FirstD._7*FirstD._5-FirstD._2*FirstD._2))*sqrt(Math.abs(FirstD._7*FirstD._6-FirstD._3*FirstD._3)))))
                          .filter(o => !o._3.isNaN)
                          .map(o => (o._1, o._2, Math.abs(o._3), signum(o._3)))
                          .groupBy(0)
                          .sortGroup(2, Order.DESCENDING)
                          .first(100)
                          .map(o => (o._2, o._3*o._4))
                          .sortPartition(0, Order.ASCENDING)

    //*******************************************************************
    //                Regression Model
    //*******************************************************************

    // filter joined clinical-genomic data
    // keeping entries for the genes with highest correlation coefficient
    // then convert table into a factor table with  column1:  gender
    //                                              column2:  age
    //                                              column3:  cancer history
    //                                              column4:  tumor size
    //                                              column5:  tumor age
    //                                              last column: local metastasis size
    //                                              rest columns: genes
    //                                         and  rows: patients
    val ultTab = joined.join(pearsons).where("geneID").equalTo(0)
                            .apply((x,y) => new GeneFact(x.file, x.geneID, x.normCount, x.gendInd, x.tumSize, x.locMet,
                                                          x.smokHis, x.age, x.tumAge))
                            .distinct
                            .groupBy(0)
                            .sortGroup(1, Order.ASCENDING)
                            .reduceGroup(redFunk(_))


    // convert factor table to MLR / CSV input
    // assigning each entry its position
    val svmIn = ultTab.map(o => (LabeledVector(o._2, DenseVector(o._1))))
                      .reduceGroup { (in, out: Collector[(Int, LabeledVector)]) => var i = 1
                                                                                    for (o <- in) {
                                                                                      out.collect((i, o))
                                                                                      i = i + 1
                                                                                    }
                                                                                  }

    // convert into train and test
    val  count = svmIn.count()
    val C1 = (count*0.1).toInt
    val C2 = (count*0.2).toInt
    //The following (commented) code was only used for Cross Validation purposes
/*
    val C3 = (count*0.3).toInt
    val C4 = (count*0.4).toInt
    val C5 = (count*0.5).toInt
    val C6 = (count*0.6).toInt
    val C7 = (count*0.7).toInt
    val C8 = (count*0.8).toInt
    val C9 = (count*0.9).toInt
    val C10 = count-C9*/

    val train1 = svmIn.filter(o => o._1 >= C2 )
      .map(o => o._2)
    val test1 = svmIn.filter(o => o._1 < C2)
      .map(o => o._2)

    //The following (commented) code was only used for Cross Validation purposes
/*
    val train2 = svmIn.filter(o => o._1 <= C2 || o._1 > C4)
      .map(o => o._2)
    val test2 = svmIn.filter(o => o._1 > C2 && o._1 <= C4)
      .map(o => o._2)

    val train3 = svmIn.filter(o => o._1 <= C4 || o._1 > C6)
      .map(o => o._2)
    val test3 = svmIn.filter(o => o._1 > C4 && o._1 <= C6)
      .map(o => o._2)

    val train4 = svmIn.filter(o => o._1 <= C6 || o._1 > C8)
      .map(o => o._2)
    val test4 = svmIn.filter(o => o._1 > C6 && o._1 <= C8)
      .map(o => o._2)
    val train5 = svmIn.filter(o => o._1 <= C8)
      .map(o => o._2)
    val test5 = svmIn.filter(o => o._1 > C8)
      .map(o => o._2)
*/

  //setting scaler
    val scaler = StandardScaler()


    // set parameters for MLR
    val mlr = MultipleLinearRegression()
      .setIterations(50)
      .setStepsize(0.05)
    val pipelineMLR = scaler.chainPredictor(mlr)
    pipelineMLR.fit(train1)
    val predictionsMLR = pipelineMLR.predict(test1)
    predictionsMLR.writeAsCsv(test4Path, "\n", "\t", WriteMode.OVERWRITE)


    //set parameters for SVM
    val svm = SVM()
      .setBlocks(10)
      .setRegularization(1)
      .setSeed(1447)

    val pipelineSVM = scaler.chainPredictor(svm)
    pipelineSVM.fit(train1)
    val predictionsSVM = pipelineSVM.predict(test1)
    predictionsSVM.writeAsCsv(test3Path, "\n", "\t", WriteMode.OVERWRITE)


    //*******************************************************************
    //                Gene Network
    //*******************************************************************

    // Finally, we create a gene network from our restricted gene list.
    // First we seperate the genes with positive correlation with metastasis
    // from the ones with negative correlation. Then for each group we do
    // the following:
    // 1. To each gene we assign a node adjusting its size according to the above
    // calculated correlation with metastasis.
    // 2. We connect two genes by an edge iff the Pearsons correlation coeff.
    // between them is bigger then 0.5.

    // calculate Pearsons correlation coefficient for each gene pair
    val geneTable = ultTab.map(o => o._1.drop(5))
    var edges : List[(Int,Int,Double)] = Nil

    // create list of genes ordered by gene ID
    val geneList = pearsons.collect()
                            .sortBy(_._1)
                            .map(_._1)

    for (i <- 0 to 99){
      for (j <- i + 1 to 99){
        val pearsInput = geneTable.map(o => (o(i), o(j)))


        val pearsCor = pearsInput.map(o => (o._1,o._2,o._1*o._2,o._1*o._1,o._2*o._2,1))
          .sum(0).andSum(1).andSum(2).andSum(3).andSum(4).andSum(5)
          .map(o => (o._6*o._3-o._1*o._2)
          /(sqrt(Math.abs(o._6*o._4-o._1*o._1))*sqrt(Math.abs(o._6*o._5-o._2*o._2))))
          .collect()

        edges = edges :+ (geneList(i), geneList(j), pearsCor(0))
      }
    }
    val edgeTable = env.fromCollection(edges)


    // create list of genes with positive Pearsons correlation coefficient
    val posGenes = pearsons.filter(o => signum(o._2) > 0)
                            .map(o => o._1)
                            .collect()

    // create list of genes with negative Pearsons correlation coefficient
    val negGenes = pearsons.filter(o => signum(o._2) < 0)
                            .map(o => o._1)
                            .collect()

    // create network for genes with positive Pearsons coefficient
    val nodeLabel = List("nodedef>name Int,pearsCorCoef DOUBLE,signum INT")
    val nodes1 = pearsons.filter(o => posGenes.contains(o._1.toString))
                    .map(o => (o._1, (Math.abs(o._2) - 0.16)*200, signum(o._2)))
                    .map(o => o._1.toString() + "," + o._2.toString + "," + o._3.toString).collect()
    val edgeLabel = List("edgedef>node1 Int,node2 Int,weight DOUBLE")
    val edges1 = edgeTable.filter(o => posGenes.contains(o._1) && posGenes.contains(o._2))
                    .filter(o => (Math.abs(o._3) > 0.5))
                    .map(o => (o._1 + "," + o._2 + "," + o._3.toString))
                    .collect()

    // export network as GDF
    val network = nodeLabel.union(nodes1).union(edgeLabel).union(edges1).map(o => Tuple1(o))
    env.fromCollection(network).writeAsCsv(netwPath, "\n", "\t", WriteMode.OVERWRITE)

    // create network for genes with positive Pearsons coefficient
    val nodes2 = pearsons.filter(o => negGenes.contains(o._1.toString))
                    .map(o => (o._1, (Math.abs(o._2) - 0.16)*200, signum(o._2)))
                    .map(o => o._1.toString() + "," + o._2.toString + "," + o._3.toString).collect()
    val edges2 = edgeTable.filter(o => negGenes.contains(o._1) && negGenes.contains(o._2))
                      .filter(o => (Math.abs(o._3) > 0.5))
                      .map(o => (o._1 + "," + o._2 + "," + o._3.toString))
                      .collect()

    // export network as GDF
    val network2 = nodeLabel.union(nodes2).union(edgeLabel).union(edges2).map(o => Tuple1(o))
    env.fromCollection(network2).writeAsCsv(netwPath2, "\n", "\t", WriteMode.OVERWRITE)


    // execute program
    env.execute("Boxel Project")
  }


  // ********************************************************************
  //                USER DATA TYPES
  // ********************************************************************


  case class ClinData0(patientId: String, gender: String, tumSize: String, locMet: String, smokHis: String,
                       smokHis2: String, daysToBirth: String,ageAtDiag: String)
  case class ClinData1(patientId: String, gender: String, tumSize: String, locMet: String, smokHis: Double,
                       daysToBirth: String, ageAtDiag: String)
  case class ClinData2(patientId: String, gendInd: Double, tumSize: Double, locMet: Double, smokHis: Double,
                       age: Double, tumAge: Double)
  case class ClinData3(file: String, gendInd: Double, tumSize: Double, locMet: Double, smokHis: Double,
                       age: Double, tumAge: Double)

  case class Aliq(file: String, sampleID: String)
  case class GenoData(geneID: String, normCount: Double)
  case class ModGenoData(file: String, geneID: String, normCount: Double)

  case class GeneFact(file: String, geneID: Int, normCount: Double, gendInd: Double,
                      tumSize: Double, locMet: Double, smokHis: Double, age: Double, tumAge: Double)

  case class Entry(factors: String, locMet: Double)
  case class Gene(geneID: Int)


  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************


  private var clinPath: String = null
  private var aliqPath: String = null
  private var genoPath: String = null
  private var test3Path: String = null
  private var test4Path: String = null
  private var netwPath: String = null
  private var netwPath2: String = null



  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 7) {
      clinPath = args(0)
      aliqPath = args(1)
      genoPath = args(2)
      test3Path =args(3)
      netwPath = args(4)
      netwPath2 = args(5)
      test4Path=args(6)

      true
    } else {
      System.err.println("Invalid input. Usage[BoxelProject]:\n" +
        " Input: BoxelProject <clinical data path> <file-sample-map path> <genomic data path> \n" +
        " Output: <SVM predictions path> <negative network path> <positive netvork path>")
      false
    }
  }

  private def getClinDataSet(env: ExecutionEnvironment): DataSet[ClinData0] = {
    env.readCsvFile[ClinData0](
      clinPath,
      fieldDelimiter = "\t",
      ignoreFirstLine = true,
      includedFields = Array(1, 6, 20, 21, 41, 45, 55, 58) )
  }

  private def getAliquots(env: ExecutionEnvironment): DataSet[Aliq] = {
    env.readCsvFile[Aliq](
      aliqPath,
      fieldDelimiter = "\t",
      includedFields = Array(0, 1)
    )
  }

  private def getGenoDataSet(env: ExecutionEnvironment, f: File): DataSet[GenoData] = {
    env.readCsvFile[GenoData](
      filePath = f.getPath,
      fieldDelimiter = "\t",
      ignoreFirstLine = true,
      includedFields = Array(0, 1) )
  }


  //*************************************************************
  //    Methods to recode cancer factors
  //*************************************************************


  private def tumSizeMap(s: String): Double = {
    if (s == "T0") { 0.0 }
    else if ((s == "T1") || (s == "T1a") || (s == "T1b")) { 0.25 }
    else if ((s == "T2") || (s == "T2a") || (s == "T2b")) { 0.5 }
    else if (s == "T3") { 0.75 }
    else if (s == "T4") { 1.0 }
    else {
      //println("Invalid tumor Size:" + s)
      return -1.0
    }
  }

  private def locMetMap(s: String): Double = {
    if (s == "N0") { 0.0 }
    else if (s == "N1") { 1.0 }
    else if (s == "N2") { 1.0 }
    else if (s == "N3") { 1.0 }
    else {
      //println("Invalid metastasise size:" + s)
      return -1.0
    }
  }

  private def genderMap(gen: String): Double = {
    if (gen == "Male") { 0.0 }
    else { 1.0 }
  }

  def smokHisNorm(o: ClinData0): ClinData1 = {
    if(o.smokHis == "Lifelong Non-smoker"){
      new ClinData1(o.patientId, o.gender, o.tumSize, o.locMet, 0.0, o.daysToBirth, o.ageAtDiag)
    }
    else if((allCatch opt o.smokHis2.toDouble).isDefined){
      new ClinData1(o.patientId, o.gender, o.tumSize, o.locMet, o.smokHis2.toDouble/100, o.daysToBirth, o.ageAtDiag)
    }
    else{
      //println(o.smokHis2)
      new ClinData1(o.patientId, o.gender, o.tumSize, o.locMet, -1.0, o.daysToBirth, o.ageAtDiag)
    }
  }


  //*************************************************************
  //    Other Methods
  //*************************************************************


  // eliminates multiple rows in the joined clinical-genomic data
  private def elim(it: Iterator[GeneFact]): GeneFact={
    var j = 0
    var g = GeneFact("s",0,0.0,0.0,0.0,0.0,0.0,0.0,0.0)
    for(i <- it){
      if(j == 0){
        g = i
      }
      j = j+1
    }
    return g
  }

  private def redFunk(input: Iterator[GeneFact]): (Array[Double], Double) = {
    var s : Seq[Double] = Nil
    var t: Seq[Double] = Nil
    var metSize: Double = 0
    var j = 0

    for(i <- input) {
      if (j == 0) {
        s = ((((s :+ i.gendInd) :+ i.age) :+ i.smokHis) :+ i.tumSize) :+ i.tumAge
        metSize = i.locMet
        j = 1
      }
      s = s :+ i.normCount
    }
    return (s.to[Array], metSize)
  }

  private def ser(in: Iterator[(Int, Int)]) : List[Int] = {
    var s: List[Int] = Nil
    for (i <- in) {
      s = s :+ i._2
    }
    return s
  }

}