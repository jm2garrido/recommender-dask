# coding: utf-8


import pandas as pd
import numpy as np
from math import sqrt
import time
import dask.dataframe as dd
from dask.distributed import Client
import sqlite3

def computeCosineSimilarity(ratingPairs):
    x = ratingPairs["rating_1"]
    y = ratingPairs["rating_2"]
    
    #sum_xx = x @ x.T
    #sum_yy = y @ y.T
    #sum_xy = x @ y.T
    sum_xx = x.dot(x.T)
    sum_yy = y.dot(y.T)
    sum_xy = x.dot(y.T)
    
    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return pd.Series({"score":score, "numPairs":ratingPairs.shape[0]})

class stopWatch():
    def __init__(self):
        self.t_value = time.perf_counter()
    
    def printTime(self, msg):
        t1 = time.perf_counter()
        print("\n{:5.2f} at {}".format(t1-self.t_value,msg))
        self.t_value = t1    

def computeMoviePairSimilarities(fileName):
    #ratings
    
    sw = stopWatch()
    
    df = dd.read_table(fileName,names = ["userID","movieID","rating","_"],usecols = ["userID","movieID","rating"],
                    dtype = {"rating":np.float64}).set_index("userID")
    
    sw.printTime("after read_table")
    df.info()
    
    
    #joinedRatings
    df = df.join(df,lsuffix='_1', rsuffix='_2')
    
    sw.printTime("after join")
    df.info()
    
    #uniqueJoinedRatings
    df = df.query("movieID_1 < movieID_2")
    sw.printTime("after filtering")
    df.info()

    
    #moviePairs
    df = df.set_index("movieID_1")
    
    #moviePairRatings
    df = df.groupby(["movieID_1","movieID_2"])
    
    sw.printTime("after groupby")    
    
    #moviePairSimilarities
    df = df.apply(computeCosineSimilarity, meta={'score': 'f8', 'numPairs': 'i8'})
    
    sw.printTime("after apply")
    df.info()
    
    df = df.compute()
    sw.printTime("after compute")
    df.info()
    
    return df

def moviePairsToSQL(conn,mps):
    conn.execute('''CREATE TABLE IF NOT EXISTS moviePairs
             (movie1 INTEGER, movie2 INTEGER, numPairs INTEGER, score REAL)''')
    conn.execute('''DROP INDEX IF EXISTS idx_movie1;''')
    conn.execute('''DROP INDEX IF EXISTS idx_movie2;''')
    conn.execute('''CREATE INDEX idx_movie1 ON moviePairs(movie1);''')
    conn.execute('''CREATE INDEX idx_movie2 ON moviePairs(movie2);''')
    conn.commit()
    
    for i in mps.itertuples(name=None):
        #print(i)
        conn.execute('''INSERT INTO moviePairs(movie1,movie2,numPairs,score)
VALUES(?,?,?,?);''',(i[0][0],i[0][1],i[2],i[1]))
    conn.commit()



def searchMovie(moviePairSimilarities,fileNames, movieID = 50, scoreThreshold = 0.97, coOccurenceThreshold = 50):    
    movieNames = pd.read_table(fileNames,names = ["movieID","title"],usecols = ["movieID","title"],
                        sep ="|",index_col = "movieID", encoding = "cp1252")
    
    
    filteredResults = moviePairSimilarities.query(
        "((movieID_1 == @movieID) or (movieID_1 == @movieID)) and (score>@scoreThreshold) and (numPairs>@coOccurenceThreshold)"
        ).sort_values(by="score",ascending=False).head(10) 
    
    
    for i,v in filteredResults.iterrows():
        if i[0] == movieID:
            recommended = i[1]
        else:
            recommended = i[0]
        nR = movieNames.loc[recommended,"title"]
        print(nR,v["score"])



#Main
if __name__ == "__main__":
    client = Client(processes=False)
    print(client)
    
    fileRatings = "ml-100k/u.data"
    fileNames = "ml-100k/u.item"
    
    moviePairSimilarities = computeMoviePairSimilarities(fileRatings)
    
    #moviePairSimilarities.to_cvs("similarities.json")

    conn = sqlite3.connect('movieNamesDask.db')

    moviePairsToSQL(conn,moviePairSimilarities)

    conn.close()

