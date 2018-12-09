#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# (c) Jose Miguel Garrido 2018
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

import sqlite3

# Constants
movieID = 2642
scoreThreshold = 0.97
coOccurenceThreshold = 500


def searchRecommendation(conn,movieID,scoreThreshold,coOccurenceThreshold):
    c = conn.cursor()
    c.execute('''SELECT * 
FROM moviePairs as mp
WHERE (mp.movie1=:movieID OR mp.movie2=:movieID) AND numPairs>:coT AND score>:sT
ORDER BY score DESC LIMIT 10;''',
              {"movieID":movieID, "coT":coOccurenceThreshold, "sT":scoreThreshold })
    return c.fetchall()

def getMovieFromID(conn,movieID):
    c = conn.cursor()
    c.execute('''SELECT * 
FROM movies
WHERE movies.movieID=?;''',(movieID,))
    return c.fetchone()

conn = sqlite3.connect('movieNamesDask.db')

rec = searchRecommendation(conn,movieID,scoreThreshold,coOccurenceThreshold )

conn.close()

conn = sqlite3.connect('movieNames1M.db')

for row in rec:
    if movieID == row[0]:
        movieRec = getMovieFromID(conn,row[1])
    else:
        movieRec = getMovieFromID(conn,row[0])
    recDic = {"movieId":movieRec[0],
              "title":movieRec[1],
              "year":movieRec[2],
              "score":row[3],
              "pairs":row[2]
             }
    print("{:1.6f} {:4} {} {}".format(recDic["score"],recDic["pairs"],recDic["title"],recDic["year"]))

conn.close()