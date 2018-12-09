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

import pandas as pd
import sqlite3

fileNames = "ml-1m/movies.dat"

movieNames = pd.read_table(fileNames,names = ["movieID","title"],usecols = ["movieID","title"],
                        sep ="::",index_col = "movieID", encoding = "cp1252",engine="python")

movieNames["year"] = movieNames["title"].str.slice(-5,-1)
movieNames["title"] = movieNames["title"].str.slice(0,-6)

print(movieNames.head())
print(movieNames.info())

conn = sqlite3.connect('movieNames1M.db')

conn.execute('''CREATE TABLE IF NOT EXISTS movies
             (movieID INTEGER PRIMARY KEY, title TEXT,year INTEGER)''')

# Create table
conn.execute('''CREATE VIRTUAL TABLE IF NOT EXISTS searchableNames 
USING FTS5(title, movieID, year);''')

conn.commit()

for i in movieNames.itertuples(name=None):
    #print(i)
    conn.execute('''INSERT INTO movies(movieID,title,year)
VALUES(?,?,?);''',i)
    conn.execute('''INSERT INTO searchableNames(title,movieID,year)
VALUES(?,?,?);''',(i[1],i[0],i[2]))

conn.commit()

conn.close()