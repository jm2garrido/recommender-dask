# coding: utf-8


import pandas as pd
import numpy as np
from math import sqrt
import time

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
        print("{:5.2} at {}".format(t1-self.t_value,msg))
        self.t_value = t1    

def computeMoviePairSimilarities(fileName):
    #ratings
    
    sw = stopWatch()
    
    df = pd.read_table(fileName, names = ["userID","movieID","rating","_"],usecols = ["userID","movieID","rating"],
                        dtype = {"rating":np.float64},index_col = "userID")
    
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
    df = df.set_index(["movieID_1","movieID_2"])
    
    #moviePairRatings
    df = df.groupby(["movieID_1","movieID_2"])
    
    sw.printTime("after groupby")    
    
    #moviePairSimilarities
    df = df.apply(computeCosineSimilarity)
    
    sw.printTime("after apply")
    df.info()
    
    return df


#Main

moviePairSimilarities = computeMoviePairSimilarities("ml-100k/u.data")

#moviePairSimilarities.to_cvs("similarities.json")

movieNames = pd.read_table("ml-100k/u.item",names = ["movieID","title"],usecols = ["movieID","title"],
                    sep ="|",index_col = "movieID", encoding = "cp1252")
movieNames.head()

movieID = 50
scoreThreshold = 0.97
coOccurenceThreshold = 50

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
