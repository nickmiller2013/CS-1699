import org.apache.spark.rdd._
import java.io._
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.hadoop.fs.{FileSystem,Path}

//The Global RDD that holds our Word, Filename, and Frequency
var wordRDD : RDD[(String, String, Double)] = sc.parallelize(List((" "," ", 0.0)))

//A timing function for the spark application
def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
}

//Get index shows the outpout of a keyword search and lists them in ranking order. 
//It can take in a single keyword or multiple keywords. 
def getIndex(){
  //Temporary rdd in order to store the outout after retrieving our keywords. 
  var tempRDD : RDD[(String, Double)] = sc.parallelize(List((" ",0.0)))

  //Reads the keywords in 
  val keyword = readLine("\nWhat is the word you are looking for? ").toString
  //println(keyword)
  //Splits the multiple keywords so that they can be searched for in our "super" wordRDD	
  val keywords = keyword.split("\\s+")
  //println(keywords)
	
  //Look through each keywords that was displayed
time { 
  for(key <- keywords){
    //Uses .filter to get only the lists from our wordRDD that match the current keyword
    //Then the mapper creates a map that is a tuple with just the doc_id and the frequency
    val queryRDD = wordRDD.filter(_._1.equals(key)).map(x => (x._2, x._3))
    //Union this queryRDD with our created tempRDD, do this so we can store all the keywords. 	  
    tempRDD = tempRDD.union(queryRDD)
  }
}	
  //Reduce all the keys so we combine the filenames that could occur from multiple keywords. There is 
  //a negative sign in front of x._2 to sort our list in ascending order instead of descending. 
  tempRDD = tempRDD.reduceByKey(_+_).filter(_._2 != 0).sortBy(x => -x._2)
  println("\n\nThe returning result for '" + keyword + "' is the following: ")
	
  //Used to print out our words 
  tempRDD.collect.foreach{ i => 
	println("Document ID: " + i._1 + " with frequency of " + i._2)
  }
  
}

//theNumber is a function that gets the absolute total of a word in all the files of the directory for a single word.
def theNumber(){

  val keyword = readLine("\nWhat is the word you would like to get total number of occurences for? (Please enter only 1 word): ").toString
  
  //queryRDD filters out wordRDD by the keyword that is being searched for but then maps it out to the keyword with the frequency
  val queryRDD = wordRDD.filter(_._1.equals(keyword)).map(x => (x._1, x._3))
  
  //Then it is reduced in to one single value
  val tempRDD = queryRDD.reduceByKey(_+_)	
	
  println("\n\nThe returning result for the total number of occurences of'" + keyword + "' is the following: ")  
  
  println("\nThe Word: " + keyword + " with frequency of " + tempRDD.lookup(keyword))
  
}

//BigData simply prints out all our data
def BigData(){
	wordRDD.collect.foreach{i => 
		println(i._1 + " -> " + i._2 + ": " + i._3)
	}
}

//bigNumber gets the percentage of times a word appears in all the documents 
def bigNumber(){
	val keyword = readLine("\nWhat is the word you would like to get total number of occurences for? (Please enter only 1 word): ").toString

  	//Filters the words by keyword then reduces to get all occurences of that word
	val tempRDD = wordRDD.filter(_._1.equals(keyword)).map(x => (x._1, x._3)).reduceByKey(_+_)

	//Reduces all frequencies (for all words) 
	val sum = wordRDD.map(_._3).reduce(_+_)
	
	
	val frequency = (tempRDD.lookup(keyword)(0)/sum) * 100

	println("\nThe total percentage that this word appears in all documents compared to all other words is: " + frequency)
}

//smallNumber gets the percentage of how many times a word appears in a certain document
//The parameter files is the RDD of our file names
def smallNumber(files: RDD[String]){
	
	var count : Int = 0
	//Move around our files RDD so we have (index, file)
	val withIndex = files.zipWithIndex
	val indexKey = withIndex.map{case (k,v) => (v,k)}
	val d = indexKey.keys
	//Print out of the file names so the user can choose they're choice 
	for(fname <- d.collect()){
		val cur_fname = indexKey.lookup(fname).mkString("").split("/").last
		println("\nEnter (" + fname + ") for filename: " + cur_fname)
	}
	println("\nEnter the corresponding number here: ")
	val number = readInt()
	
	//Find the filename of which the user input matches (by index)
	val file2b = indexKey.lookup(number).mkString("").split("/").last

	//Filter all tuples and return only tuples with the recorded filename
	val totalFromDoc = wordRDD.filter(_._2.equals(file2b)).map(_._3).reduce(_+_)
	val keyword = readLine("\nWhat is the word you would like to get total number of occurences for? (Please enter only 1 word): ").toString

	//Filters all tuples by the filename and our keyword to return only occurences of the keyword in the particular document
  	val keyFromDoc = wordRDD.filter(_._1.equals(keyword)).filter(_._2.equals(file2b)).map(_._3).reduce(_+_)

        val frequency = (keyFromDoc/totalFromDoc) * 100

        println("\nThe total percentage that this word appears in the document " + file2b + " is: " + frequency)	


}
println("\nWelcome to Tiny Google! The Files are being loaded in!\n\n")

	//wholeTextFiles reads in a RDD of the filename as key and text from book as value
	var files = sc.wholeTextFiles("hdfs://had6110.cs.pitt.edu:8020/user/chatree/CS1699/Books/*")
	//Gets the keys from our files RDD 
	var a = files.keys
time { 
	for(name <- a.collect()){ //Look through all our files 
	  val orig_text = files.lookup(name) //Get the text from our files 
	  val mapped = sc.parallelize(orig_text) //Push our text into an rdd
	  val returnText = mapped.flatMap(l => l.split(' ')) //These next are properties of wordcount
	  .map(_.toLowerCase()) //Convert to lowercase 
	  .map(word => word.filter(Character.isLetter(_))) // Only get letters instead of punctuation -- For basic use
	  .map(word => (word,1)) //Give each word a 1 value
	  .reduceByKey(_+_) //Get the frequency of each word 
	  val fname = name.split("/").last //Get the actual filename from our list of files keys
	  //Create an RDD to store all of the groups of (word, file, frequency)
	  val toBeAdded : RDD[(String, String, Double)] = returnText.map(word => (word._1, fname, word._2)) 
	  //Union our adding RDD to our global RDD so all files will be in single RDD 
	  wordRDD = wordRDD.union(toBeAdded)
	}
}
	//Get rid of the files variable in memory to create a better efficiency for the program. 
	//wholeTextFiles takes up a lot of space 
	files.unpersist()	

println("\nWelcome to Tiny Google! The Files have been loaded in!!\n\n")	

//UI to determine what the user wants to do. 
var keyword : String = "regular"
	keyword = readLine("\nEnter 'run' to run the program or 'stop' to stop the program: ")
	while(keyword.equals("run")){
		println("\n1.) Get All the Words, the document they appear in, and the specific document frequency. Enter 'BigData'")
		println("\n2.) Get The frequency of a specific word for all the documents. Enter 'theNumber'")	
		println("\n3.) Get the percentage of a word to in all documents to all words in all documents. Enter 'bigNumber'")
		println("\n4.) Get the percentage of a word in a document to all word in the ONE document. Enter 'smallNumber'")
		println("\n5.) Do what this program was meant for and enter a keyword and return the associated\n file and frequencies it occurs in. Enter 'getIndex'")
		
		var enterText = readLine("\nEnter the option here: ")
		if(enterText.equals("BigData")){
			time {BigData()}

		}else if(enterText.equals("theNumber")){
			time {theNumber()}

		}else if(enterText.equals("bigNumber")){
			time {bigNumber()}

		}else if(enterText.equals("smallNumber")){
			time {smallNumber(a)}
			
		}else if(enterText.equals("getIndex")){
			time {getIndex()}
			
		}
		keyword = readLine("\nEnter 'run' to run the program or 'stop' to stop the program: ")

}

