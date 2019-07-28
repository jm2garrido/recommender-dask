# coding: utf-8

# (c) Jose Miguel Garrido
# Based on algorithm from https://www.udemy.com/taming-big-data-with-apache-spark-hands-on/ from Frank Kane
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.

#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.

#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.

import pandas as pd
import numpy as np
from math import sqrt
import time
import dask.dataframe as dd
from dask.distributed import Client

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
    
    df = dd.read_csv(fileName,names = ["userID","movieID","rating","_"],usecols = ["userID","movieID","rating"],
                    dtype = {"rating":np.float64}).set_index("userID")
    
    sw.printTime("after read_table")
    df.info()
    print("\n Partitions {}".format(df.npartitions))
    df = df.repartition(npartitions = df.npartitions * 100)
    
    #joinedRatings
    df = df.join(df,lsuffix='_1', rsuffix='_2')
    
    sw.printTime("after join")
    df.info()
    
    #uniqueJoinedRatings
    df = df.query("movieID_1 < movieID_2")
    sw.printTime("after filtering")
    df.info()

    
    #moviePairs
    df = df.set_index("movieID_1","movieID_2")

    #some operation for preparing the cosineSimilarity
    df["rating_xy"] = df["rating_1"] * df["rating_2"]
    df["rating_xx"] = df["rating_1"] * df["rating_1"]
    df = df.drop("rating_1", axis = 1)
    df["rating_yy"] = df["rating_2"] * df["rating_2"]
    df = df.drop("rating_2", axis = 1)
        
    sw.printTime("after squaring")
    df.info()

    #moviePairRatings
    df = df.groupby(["movieID_1","movieID_2"])
    print(type(df))
    
    sw.printTime("after groupby")    

    #df = client.persist(df)

    totals = df["rating_xy"].count()
    #print(totals.head())
    
    sw.printTime("after count")
    
    #moviePairSimilarities
    
    df = df.sum()
    
    df["score"] = df["rating_xy"] / (df["rating_xx"].pow(0.5) * df["rating_yy"].pow(0.5) )
    df = df.drop(["rating_xy","rating_xx","rating_yy"], axis = 1)
    
    df["numPairs"] = totals
    
    sw.printTime("after cosineSimilarity")
    
    df.to_csv('export-*.csv') 
    df = df.compute()
    sw.printTime("after compute")
    df.info()
    
    return df


#Main
if __name__ == "__main__":
    client = Client(processes=False)
    print(client)
    
    fileRatings = "ml-100k/u.data"
    fileNames = "ml-100k/u.item"
    
    moviePairSimilarities = computeMoviePairSimilarities(fileRatings)
    
    #moviePairSimilarities.to_cvs("similarities.json")
    
    movieNames = pd.read_csv(fileNames,names = ["movieID","title"],usecols = ["movieID","title"],
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
