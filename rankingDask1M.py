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
import time
import dask.dataframe as dd
from dask.distributed import Client




class stopWatch():
    def __init__(self):
        self.t_value = time.perf_counter()
    
    def printTime(self, msg):
        t1 = time.perf_counter()
        print("\n{:5.2f} at {}".format(t1-self.t_value,msg))
        self.t_value = t1    

def computeImdbRanking(fileName,m):
    #ratings
    
    sw = stopWatch()
    
    df = dd.read_table(fileName,names = ["userID","movieID","rating","_"],usecols = ["movieID","rating"],
                    dtype = {"rating":np.float64}, sep ="::",engine="python").set_index("movieID")
    
    sw.printTime("after read_table")
    df.info()
    print("\n Partitions {}".format(df.npartitions))
    #df = df.repartition(npartitions = df.npartitions * 80)
    
    df = df.groupby("movieID")["rating"].agg(["mean","count"]).rename(columns = {"mean":"R","count":"v"})
    
    
    sw.printTime("after groupBy")
    df.info()
    
    df = df.persist()    

    
    C = df["R"].mean().compute()
    print(C)
    
    mc = m * C
    
    df["score"] = (( df["v"] * df["R"]) + mc )/ ( df["v"] + m ) 
        
    df = df.compute()
    
    sw.printTime("after compute")
    df.info()
    
    
    
    return df


#####Main
    
if __name__ == "__main__":
    client = Client(processes=False)
    print(client)
    
    # for 1M
    #fileRatings = "ml-1m/ratings.dat"
    #fileNames =   "ml-1m/movies.dat"
    #encoding = "cp1252"
    
    # for 10M
    fileRatings = "ml-10M100K/ratings.dat"
    fileNames =   "ml-10M100K/movies.dat"
    encoding = "utf-8"
    
    m = 6000
    
    movieRankings = computeImdbRanking(fileRatings,m)
    
    movieRankings = movieRankings.sort_values("score", ascending = False)
    
    #print(movieRankings.head())
    print(movieRankings.info())    

    
    movieNames = pd.read_table(fileNames,names = ["movieID","title"],usecols = ["movieID","title"],
                        sep ="::",index_col = "movieID", encoding = encoding, engine="python")
    #movieNames.head()
    print(movieNames.info())        

    
    
    for i,v in movieRankings.iloc[0:10].iterrows():
        print("{:f} {:f} {:d} {}".format(v["score"],v["R"],int(v["v"]),movieNames.loc[i,"title"]))
    
#    for i,v in filteredResults.iterrows():
#        if i[0] == movieID:
#            recommended = i[1]
#        else:
#            recommended = i[0]
#        nR = movieNames.loc[recommended,"title"]
#        print(nR,v["score"])
