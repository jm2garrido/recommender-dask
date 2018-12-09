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

movieKeywords = "superman"

def getMovieFromFreeText(conn,text):
    c = conn.cursor()
    c.execute('''SELECT * 
FROM searchablenames
WHERE searchablenames.title MATCH ? ORDER BY rank;''',(text,))
    return c.fetchall()

def searchRecommendation(conn,movieID,scoreThreshold,coOccurenceThreshold):
    c = conn.cursor()
    c.execute('''SELECT * 
FROM moviePairs as mp
WHERE (mp.movie1=:movieID OR mp.movie2=:movieID) AND numPairs>:coT AND score>:sT
ORDER BY score DESC LIMIT 10;''',
              {"movieID":movieID, "coT":coOccurenceThreshold, "sT":scoreThreshold })
    return c.fetchall()


conn = sqlite3.connect('movieNames1M.db')

for i in getMovieFromFreeText(conn,movieKeywords):
    print("{} {} {}".format(i[1],i[0],i[2]))

conn.close()