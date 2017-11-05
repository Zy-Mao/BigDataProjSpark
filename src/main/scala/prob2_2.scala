import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.Vectors

object prob2_2 extends App{
  val conf = new SparkConf().setMaster("local").setAppName("My App")
  val sc = new SparkContext(conf)
  val input = sc.textFile("/Home/Documents/project3/points")
  val points = input.map(p => Vectors.dense(p.split(',').map(_.toDouble))).map(x => {x(0).toInt / 20 + (10000 - x(1).toInt) / 20 * 500 + 1})
  val cells = points.countByValue().map(x => (x._1,x._2.toDouble))
  val cellList = sc.parallelize(cells.toList)
  val pointsMap = sc.broadcast(cellList.collectAsMap)
  val I = cells.map(cell => (cell._1,Density(cell._1)))
  val M = I.toList.sortBy(_._2)
  var list= Map[Int,Double]()

  for(i <- M.length-101 until M.length -1 ){
    list += M(i)
    println(M(i))
  }
  for(i <- list){
    val li = Neighbors(i._1)
    for(j <- li)
      print("(" + j + "," + I.get(j) + ")")
    println()
  }

  // gather the neighbors to a list 
  def Neighbors(cell:Int) : List[Int]= {
    var neighbors = List[Int]()
    if (cell == 1) {
      neighbors = List(2, 501, 502)
    } else if (cell == 500) {
      neighbors = List(499, 999, 1000)
    } else if (cell < 500) {
      neighbors = List(cell-1, cell+1, cell+499, cell+500, cell+501)
    } else if (cell == 249501) {
      neighbors = List(249001, 249002, 249502)
    } else if (cell == 250000) {
      neighbors = List(249499, 249500, 249999)
    } else if (cell > 249501) {
      neighbors = List(cell-1, cell+1, cell-499, cell-500, cell-501)
    } else if (cell % 500 == 1) {
      neighbors = List(cell+1, cell+500, cell+501, cell-500, cell-499)
    } else if (cell % 500 == 0) {
      neighbors = List(cell-1, cell+500, cell+499, cell-500, cell-501)
    } else {
      neighbors = List(cell-1, cell+1, cell-499, cell-500, cell-501, cell+499, cell+500, cell+501)
    }
    neighbors
  }

  // calculate relative density scores for cells
  def Density(cell:Int) : Double= {
    var avg : Double = 0
    if (cell == 1) {
      avg = (pointsMap.value.getOrElse[Double](2, 0) + pointsMap.value.getOrElse[Double](501, 0) + pointsMap.value.getOrElse[Double](502, 0)) / 3
    } else if (cell == 500) {
      avg = (pointsMap.value.getOrElse[Double](499, 0) + pointsMap.value.getOrElse[Double](999, 0) + pointsMap.value.getOrElse[Double](1000, 0)) / 3
    } else if (cell < 500) {
      avg = (pointsMap.value.getOrElse[Double](cell-1, 0) + pointsMap.value.getOrElse[Double](cell+1, 0) + pointsMap.value.getOrElse[Double](cell+499, 0)
            + pointsMap.value.getOrElse[Double](cell+500, 0) + pointsMap.value.getOrElse[Double](cell+501, 0)) / 5
    } else if (cell == 249501) {
      avg = (pointsMap.value.getOrElse[Double](249001, 0) + pointsMap.value.getOrElse[Double](249002, 0) + pointsMap.value.getOrElse[Double](249502, 0)) / 3
    } else if (cell == 250000) {
      avg = (pointsMap.value.getOrElse[Double](249499, 0) + pointsMap.value.getOrElse[Double](249500, 0) + pointsMap.value.getOrElse[Double](249999, 0)) / 3
    } else if (cell > 249501) {
      avg = (pointsMap.value.getOrElse[Double](cell-1, 0) + pointsMap.value.getOrElse[Double](cell+1, 0) + pointsMap.value.getOrElse[Double](cell-499, 0)
            + pointsMap.value.getOrElse[Double](cell-500, 0) + pointsMap.value.getOrElse[Double](cell-501, 0)) / 5
    } else if (cell % 500 == 1) {
      avg = (pointsMap.value.getOrElse[Double](cell+1, 0) + pointsMap.value.getOrElse[Double](cell+500, 0) + pointsMap.value.getOrElse[Double](cell+501, 0)
            + pointsMap.value.getOrElse[Double](cell-500, 0) + pointsMap.value.getOrElse[Double](cell-499, 0)) / 5
    } else if (cell % 500 == 0) {
      avg = (pointsMap.value.getOrElse[Double](cell-1, 0) + pointsMap.value.getOrElse[Double](cell+500, 0) + pointsMap.value.getOrElse[Double](cell+499, 0)
            + pointsMap.value.getOrElse[Double](cell-500, 0) + pointsMap.value.getOrElse[Double](cell-501, 0)) / 5
    } else {
      avg = (pointsMap.value.getOrElse[Double](cell-1, 0) + pointsMap.value.getOrElse[Double](cell+1, 0) + pointsMap.value.getOrElse[Double](cell-499, 0)
            + pointsMap.value.getOrElse[Double](cell-500, 0) + pointsMap.value.getOrElse[Double](cell-501, 0) + pointsMap.value.getOrElse[Double](cell+499, 0)
            + pointsMap.value.getOrElse[Double](cell+500, 0) + pointsMap.value.getOrElse[Double](cell+501, 0)) / 8
    }
    pointsMap.value.getOrElse[Double](cell, 0) / avg
  }
}
