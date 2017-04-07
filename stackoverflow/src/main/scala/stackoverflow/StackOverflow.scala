package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.reflect.ClassTag

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[Int], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
//    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}


/** The parsing and kmeans methods */
class StackOverflow extends Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
              parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(Int, Iterable[(Posting, Posting)])] = {
  	//filters to leave only question postings together and creates pairing of (question id, question post)
    val questions = postings.filter(posting => posting.postingType == 1).map(questionPost => (questionPost.id, questionPost))
    //filters to leave only answer postings together and creates pairing of (parent id, answer post). Throws out those answers missing question parent id
    val answers = postings.filter(posting => posting.postingType == 2).map(answerPost => (answerPost.parentId, answerPost)).filter(pair => !pair._1.isEmpty).map(pair => (pair._1.get, pair._2))
    //joins questions and answers on the corresponding question id and groups by question id key
    questions.join(answers).groupByKey()
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(Int, Iterable[(Posting, Posting)])]): RDD[(Posting, Int)] = {

    def answerHighScore(as: Array[Posting]): Int = {
      var highScore = 0
          var i = 0
          while (i < as.length) {
            val score = as(i).score
                if (score > highScore)
                  highScore = score
                  i += 1
          }
      highScore
    }

    //produces a pair RDD where the first item is the question post and the second item is the Array of answers to that question
    val questionVsAnswers = grouped.map(x => (x._2.head._1, x._2.map(y => y._2).toArray))
    questionVsAnswers.mapValues(as => answerHighScore(as))
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Posting, Int)]): RDD[(Int, Int)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }

  
    scored.map(postScorePair => (firstLangInTag(postScorePair._1.tags, langs), postScorePair._2))
    .filter(pair => !pair._1.isEmpty)
    .map(pair => (pair._1.get * langSpread, pair._2)).cache()
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(Int, Int)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {

  	@tailrec def kmeansIteration(means: Array[(Int, Int)], iter: Int, debug: Boolean = false): Array[(Int, Int)] = {
  		val newMeans = means.clone()

	  	//New rdd of pairs with int of closest cluster to v and v 
	    val closestClusterToEachVector = vectors.map(v => (findClosest(v, means), v))
	    //Group the vectors by the cluster number. 
	    val vectorsGroupedByCluster = closestClusterToEachVector.groupByKey()
	    //For each cluster # and Iterable(vectors) pair, take just the average of the vectors in the Iterable, then convert the RDD of these average vectors to an array
	    //This gives the new means
	    val newClustersWithMeans = vectorsGroupedByCluster.mapValues(vs => averageVectors(vs)).collect()
	    newClustersWithMeans.foreach(pair => newMeans(pair._1) = pair._2)
	    // Check how large the total shift was in cluster centroids over this iteration
	    val distance = euclideanDistance(means, newMeans)

	    if (debug) {
	      println(s"""Iteration: $iter
	                 |  * current distance: $distance
	                 |  * desired distance: $kmeansEta
	                 |  * means:""".stripMargin)
	      for (idx <- 0 until kmeansKernels)
	      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
	              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
	    }

	    if (converged(distance))
	      newMeans
	    else if (iter < kmeansMaxIterations)
	      kmeansIteration(newMeans, iter + 1, debug)
	    else {
	      println("Reached max iterations!")
	      newMeans
	    }

  	}

  	vectors.cache()
  	kmeansIteration(means, iter, debug)
  }




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the sum of the euclidean distance between two arrays of points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the integer index of the center closest to the given point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }




  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(Int, Int)]): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey().cache()

    val median = closestGrouped.mapValues { vs =>
      // identifies the index in langs of the most common language in the most common language in the cluster
      val langLabel: String   = {
      	// computes a list of tuple pairs for the cluster, consisting of the integer index of the lang of the question and the frequency of the lang,
      	// sorted in descending order by lang
      	val langsWithFreqsSortedDesc = vs.map(p => p._1 / langSpread).groupBy(x => x).mapValues(iter => iter.size).toList.sortWith((a,b) => a._2 > b._2)
      	// gets the index of the lang with the highest frequency in the cluster
      	val idx = langsWithFreqsSortedDesc.head._1
      	langs(idx)
      	}
      // percent of the questions in the cluster in the most common language 
      val langsWithFreqsSortedDesc = vs.map(p => p._1 / langSpread).groupBy(x => x).mapValues(iter => iter.size).toList.sortWith((a,b) => a._2 > b._2)
      val langPercent: Double = (langsWithFreqsSortedDesc.head._2.toDouble / vs.size.toDouble)*100
      
      val clusterSize: Int    = vs.size
      val medianScore: Int    =  {
      	// Creates a list of (lang score, answer score) pairs, sorted descending by answer score
      	val langsWithScoresSortedDescByScores = vs.toList.sortWith((a,b) => a._2 > b._2)
      	//computes and returns the median score; median computed differently depending on whether set has odd or even number of elements
      	// odd clusterSize -> middle value
      	if (clusterSize % 2 == 1) langsWithScoresSortedDescByScores(clusterSize / 2)._2
      	//even clusterSize -> mean of two middle values
      	else (langsWithScoresSortedDescByScores(clusterSize / 2)._2 + langsWithScoresSortedDescByScores(clusterSize / 2 - 1)._2) / 2
      	// Then compute the index medianIndex of the median and then get langsWithScoresSortedDescByScores(medianIndex)._2
      }

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
