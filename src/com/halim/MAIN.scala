package com.halim
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import java.lang.Boolean
import org.apache.spark.sql.catalyst.expressions.Length

object MAIN {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("big-data")
      .getOrCreate()
     //pallindrom(spark)
     //voyelleEx2EXAM2020(spark) 
     //ADNTripleEx1EXEM20202(spark)
      anagramme(spark)
    spark.stop()
  }
  def anagramme(sc:SparkSession){
    val anagrame = "C:/Users/X1/Desktop/master - S2/big-data/spark/spark-workspace/big-data-test/src/anagrame.txt" //
     val data = sc.sparkContext.textFile(anagrame)
     val wordsRDD = data.flatMap(line => line.trim().split(" ").iterator)
     val wordsAndSortedWord = wordsRDD.map(word => (word.sorted.toLowerCase(),word))
     val reducedWords = wordsAndSortedWord.reduceByKey((str1,str2) => str1.trim()+ " "+str2.trim())
     .filter(f => f._2.contains(" "))
     .map(w => w._2).foreach(println)
     
  }
  /*################################################################*/
  def ADNTripleEx1EXEM20202(sc:SparkSession){
     val adntrplet = "C:/Users/X1/Desktop/master - S2/big-data/spark/spark-workspace/big-data-test/src/adn.txt" //
     val data = sc.sparkContext.textFile(adntrplet)
     val result = question1(data);
     
     question2(result);
     question3(result);
  }
  def question1(data:RDD[String]) : RDD[(String,Int)]={
    
    val tripletRDDArray = data.map(line => claculateTripletNumber(line.trim()))
     val tripletsRDDTuple =  tripletRDDArray.map(ar => ar.map(el => (el,1))).flatMap(f => f.iterator)
     .reduceByKey((int1 , int2) => int1 + int2)
      tripletsRDDTuple.foreach(tuple => println(tuple._1 +"   "+ tuple._2))
      return tripletsRDDTuple;
  }
  def claculateTripletNumber(line:String):Array[String]={
    var triplets = Array[String]()
    for(i<-0 until line.length()-3){
      val triplet = line.substring(i,i+3)
      triplets = triplets:+triplet
    }
    return triplets
  }
  def question2(resultq1:RDD[(String,Int)]){
    println("##################################################################")
    val values = resultq1.map(tuple => tuple._2).sum()
    val tripletfrequence = resultq1.map(tuple => (tuple._1,tuple._2/values)).filter(el => el._2 > 0.50)
    .foreach(f => println(f._1 + "  "+ f._2 +"%"))
     println("##################################################################")
  }
  def question3(resultq1:RDD[(String,Int)]){
    val tripletsRDD = resultq1.map(tuple => tuple._1).map(el => (el,comeplement(el)))
    .foreach(tuple => println(tuple._1 + "  "+ tuple._2))
  }
  def comeplement(triplet:String):String={
    var complement = Array[Char]();
    for(i<-0 until triplet.length()){
      val el = triplet(i)
      if(el == 'T')
       complement = complement:+'A'
       if(el == 'A')
       complement = complement:+'T'
       if(el == 'C')
       complement = complement:+'G'
        if(el == 'G')
       complement = complement:+'C'
       
    }

    return complement.mkString("")
    
  }
  ////////////////////////////////////// VOYELLES ///////////////////////////////////////
  def voyelleEx2EXAM2020(sc:SparkSession){
     val logFile = "C:/Users/X1/Desktop/master - S2/big-data/spark/spark-workspace/big-data-test/src/voyell.txt" //
     val data = sc.sparkContext.textFile(logFile)
     val linesArrayRDD = data.map(line => findWordsWith3Voyels(line.trim()))
      //linesArrayRDD.foreach(arryrdd => println(arryrdd.mkString(" ")))
     val voyelsTupleWords =  linesArrayRDD.map(ar => ar.map(el => (el,1))).flatMap(f => f.iterator)
     .reduceByKey((int1 , int2) => int1 + int2)
      voyelsTupleWords.foreach(tuple => println(tuple._1 +"   "+ tuple._2))
     
  }
  def findWordsWith3Voyels(line: String) : Array[String]={
    var words = Array[String]()
    val lineToArray = line.split(" ")
    
    for( i <-0 until lineToArray.length){
      val word = lineToArray(i)
      val bool = doesContain3Voyell(word)
      if(bool){
        
        words = words:+ word
      }
    }
    return words
  }
  def doesContain3Voyell(word:String) : Boolean={
    val voyels: Array[Char] = Array('e','a','i','o','y','u')
    var voyellsInWord  = Array[Char]()
    for(i<- 0 until word.length())
    {
      for(j<-0 until voyels.length){
         val voy = voyels(j)
        if(word(i).toLower==voy.toLower)
           voyellsInWord = voyellsInWord:+voy
      }
    }
    
    if(voyellsInWord.length >= 3 ) {
      
      return true
    }
    
    else 
      return false
    
  }
  ////////////////////// PALLINDROM ///////////////////////////////////
  def pallindrom(sc:SparkSession){
     val logFile = "C:/Users/X1/Desktop/master - S2/big-data/spark/spark-workspace/big-data-test/src/pallindrom.txt" //
     val data = sc.sparkContext.textFile(logFile)
     
     val arrayOfPallindromsRDD = data.map(line => findPallindrom(line.trim()))
      arrayOfPallindromsRDD.foreach(arryrdd => println(arryrdd.mkString(" ")))
     val apallindromWords =  arrayOfPallindromsRDD.map(ar => ar.map(el => (el,1))).flatMap(f => f.iterator)
     .reduceByKey((int1 , int2) => int1 + int2)
     
     apallindromWords.foreach(tuple => println(tuple._1 +"   "+ tuple._2))
     
  }
  def findPallindrom(line:String): Array[String]={
    var words = Array[String]()
    val WordsInline: Array[String] = line.trim().split(" ")
    for( i <- 0 until (WordsInline.length)){
      val word = WordsInline(i).trim()
      val bool = isPallindrom(word)
      if(bool){
       words=words:+word
    }
  }
    return words
    
  }
  def isPallindrom(word: String): Boolean = {
    val len = word.trim().length();
    if (len > 0) {
        for (i <- 0 to (len / 2)) {
          if (!word.substring(i, i + 1).toLowerCase().equals(word.substring(len - 1 - i, len - i).toLowerCase())) {
            return false
          }
        }
    }
    return true
  }
  ////////////////////////////////////////////////////////////////
  
  
}