from pyspark.sql import SparkSession # This is for Spark 2.0 that has both SparkContext and SQLContext
from pyspark.sql import Row # Create DataFrames of Row object
from pyspark.sql import functions

def loadMovieNames():
    movieNames = {} 
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def parseInput(line):
    fields = line.split() # One line at at time. 
    return Row(movieID = int(fields[1]), rating = float(fields[2])) #RDD of rows that contains MovieID and Ratings

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate() #getorCreate - Recover from the cells. 

    # Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()

    # Get the raw data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data") #sparksession contain sparkContext
    # Convert it to a RDD of Row objects with (movieID, rating)
    movies = lines.map(parseInput) 
    # Convert that to a DataFrame
    movieDataset = spark.createDataFrame(movies) # Convert Rdd into a Dataframe

    # Compute average rating for each movieID
    averageRatings = movieDataset.groupBy("movieID").avg("rating") #Does a reduction by MovieID and average the ratings

    # Compute count of ratings for each movieID
    counts = movieDataset.groupBy("movieID").count() # MovieIDs and Counts

    # Join the two together (We now have movieID, avg(rating), and count columns)
    averagesAndCounts = counts.join(averageRatings, "movieID") 

    # Pull the top 10 results
    topTen = averagesAndCounts.orderBy("avg(rating)").take(10)

    # Print them out, converting movie ID's to names as we go.
    for movie in topTen:
        print (movieNames[movie[0]], movie[1], movie[2])

    # Stop the session
    spark.stop()
