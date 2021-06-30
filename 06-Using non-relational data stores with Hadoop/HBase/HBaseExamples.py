from starbase import Connection

c = Connection("127.0.0.1", "8000") #Connect to REST Service. 

ratings = c.table('ratings')

if (ratings.exists()):
    print("Dropping existing ratings table\n")
    ratings.drop()

ratings.create('rating')

print("Parsing the ml-100k ratings data...\n")
ratingFile = open("C:/Users/Corbi/Desktop/Programming/Hadoop/1. Introduction (Hortonworks Data Platform)/ml-100k/u.data", "r")

batch = ratings.batch() #Import into HBASE by batch it all. 

for line in ratingFile:
    (userID, movieID, rating, timestamp) = line.split()
    batch.update(userID, {'rating': {movieID: rating}}) #Construct the Column Family containing the MovieID and their Rating


ratingFile.close()

print ("Committing ratings data to HBase via REST service\n")
batch.commit(finalize=True)

print ("Get back ratings for some users...\n")
print ("Ratings for user ID 1:\n")
print (ratings.fetch("1")) # UserID 1
print ("Ratings for user ID 33:\n")
print (ratings.fetch("33")) #

ratings.drop()
