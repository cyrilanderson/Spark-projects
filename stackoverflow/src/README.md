In this assignment, focused on the PairRDD data structure using the Spark Scala API.

In this assignment, we used a dataset consisting of questions and answers from Stackoverflow relating to several different programming languages (CSS, Groovy, C#, Ruby, PHP, Objective-C, Java, MATLAB, Javascript, C++, Perl, Python, Clojure, Scala, Haskell)

The questions were brought together with their corresponding answers by matching question ids for questions with parent ids for answers. 
Then the highest scoring answers were identified for each question.
Next, for each question, created pair consisting of the integer id of the language to which the question relates and the highest answer score.
Then, (Int, Int) pairs of (language id * spread factor, score) were used as vectors and run through a K-means clustering algorithm.
The adjustable spread factor weights how important the language id is for computing the distance between vectors, which plays a role in the contents of the clusters.

The results for each cluster are then analyzed.

The starting data can be downloaded from 
http://alaska.epfl.ch/~dockermoocs/bigdata/wikipedia.dat
To run, place the data in the folder: stackoverflow/src/main/resources/wikipedia in the project directory.
