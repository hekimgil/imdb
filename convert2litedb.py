# -*- coding: utf-8 -*-
"""
Python code to read .tsv İMDB data and store it in a sqlite database

@author: Hakan Hekimgil
"""

debug = False
debuglimit = 2000000
titlesDone = False
peopleDone = False

step1 = 25000
step2 = 10000
step3 = 100000
step4 = 50000
step5 = 100000

folder = "data/"
titlesfile = "title.basics.tsv"
ratingsfile = "title.ratings.tsv"
namesfile = "name.basics.tsv"
crewfile = "title.crew.tsv"
principalsfile = "title.principals.tsv"


import sqlite3
# connect to database
conn = sqlite3.connect("data/imdblite.db")

db = conn.cursor()
# make sure the tables exist
db.execute("DROP TABLE IF EXISTS titles;")
db.execute("DROP TABLE IF EXISTS genres;")
db.execute("DROP TABLE IF EXISTS people;")
db.execute("DROP TABLE IF EXISTS professions;")
db.execute("DROP TABLE IF EXISTS ratings;")
db.execute("DROP TABLE IF EXISTS directors;")
db.execute("DROP TABLE IF EXISTS writers;")
db.execute("DROP TABLE IF EXISTS principals;")
db.execute("DROP TABLE IF EXISTS categories;")
print("Checking/setting database tables..")
db.execute(
        "CREATE TABLE IF NOT EXISTS titles (" + 
        "tconst INTEGER PRIMARY KEY, primaryTitle TEXT, " + 
        "startYear INTEGER, endYear INTEGER, runtimeMinutes INTEGER, " + 
        "genre1 INTEGER, genre2 INTEGER, genre3 INTEGER, " + 
        "FOREIGN KEY (genre1) REFERENCES genres(id) ON DELETE SET NULL, " + 
        "FOREIGN KEY (genre2) REFERENCES genres(id) ON DELETE SET NULL, " + 
        "FOREIGN KEY (genre3) REFERENCES genres(id) ON DELETE SET NULL);")
db.execute(
        "CREATE TABLE IF NOT EXISTS genres (" + 
        "id INTEGER PRIMARY KEY, genre TEXT);")
db.execute(
        "CREATE TABLE IF NOT EXISTS people (" + 
        "nconst INTEGER PRIMARY KEY, primaryName TEXT, birthYear INTEGER, deathYear INTEGER," + 
        "prof1 INTEGER, prof2 INTEGER, prof3 INTEGER);")
db.execute(
        "CREATE TABLE IF NOT EXISTS professions (" + 
        "id INTEGER PRIMARY KEY, profession TEXT);")
db.execute(
        "CREATE TABLE IF NOT EXISTS ratings (" + 
        "tconst INTEGER NOT NULL, averageRating REAL, numVotes INTEGER, " + 
        "FOREIGN KEY (tconst) REFERENCES titles(tconst));")
db.execute(
        "CREATE TABLE IF NOT EXISTS directors (" + 
        "tconst INTEGER NOT NULL, director INTEGER, " + 
        "FOREIGN KEY (tconst) REFERENCES titles(tconst)," + 
        "FOREIGN KEY (director) REFERENCES people(nconst));")
db.execute(
        "CREATE TABLE IF NOT EXISTS writers (" + 
        "tconst INTEGER NOT NULL, writer INTEGER, " + 
        "FOREIGN KEY (tconst) REFERENCES titles(tconst)," + 
        "FOREIGN KEY (writer) REFERENCES people(nconst));")
db.execute(
        "CREATE TABLE IF NOT EXISTS principals (" + 
        "tconst INTEGER NOT NULL, ordering INTEGER NOT NULL, nconst INTEGER NOT NULL, " + 
        "catid INTEGER, " + 
        "FOREIGN KEY (tconst) REFERENCES titles(tconst)" + 
        "FOREIGN KEY (nconst) REFERENCES people(nconst)" + 
        "FOREIGN KEY (catid) REFERENCES categories(id));")
db.execute(
        "CREATE TABLE IF NOT EXISTS categories (" + 
        "id INTEGER PRIMARY KEY, category TEXT);")
# commit the current state
conn.commit()
print("Database tables set..")

# start reading and moving to db
# transfer titles
print("Transfering titles...")
titleset = set()
peopleset = set()
with open(folder+titlesfile) as tfile:
    names = tfile.readline()[:-1].split("\t")
    line = "\n"
    count = 0
    while line[-1] == "\n":
        line = tfile.readline()
        if len(line) == 0:
            break
        #print(line)
        [tconst, ttype, title, ortitle, adult, startY, endY, runtime, gens] = line.split("\t")
        if (int(adult) == 0) and (ttype == "Movie" or ttype == "tvMovie"):
            count += 1
            titleset.add(int(tconst[2:]))
            if startY == "\\N":
                startY = "NULL"
            else:
                startY = int(startY)
            if endY == "\\N":
                endY = "NULL"
            else:
                endY = int(endY)
            if runtime == "\\N":
                runtime = "NULL"
            else:
                runtime = int(runtime)
            gen1 = "NULL"
            gen2 = "NULL"
            gen3 = "NULL"
            gens = gens.strip("\n")
            if gens != "\\N":
                gens = gens.split(",")
                gentext = gens[0]
                db.execute("SELECT id FROM genres WHERE genre = ?", (gentext,))
                genid = db.fetchone()
                if genid == None:
                    db.execute("INSERT INTO genres (genre) VALUES (?)", (gentext,))
                    conn.commit()
                    db.execute("SELECT id FROM genres WHERE genre = ?", (gentext,))
                    genid = db.fetchone()
                gen1 = int(genid[0])
                if len(gens) > 1:
                    gentext = gens[1]
                    db.execute("SELECT id FROM genres WHERE genre = ?", (gentext,))
                    genid = db.fetchone()
                    if genid == None:
                        db.execute("INSERT INTO genres (genre) VALUES (?)", (gentext,))
                        conn.commit()
                        db.execute("SELECT id FROM genres WHERE genre = ?", (gentext,))
                        genid = db.fetchone()
                    gen2 = int(genid[0])
                    if len(gens) > 2:
                        gentext = gens[2]
                        db.execute("SELECT id FROM genres WHERE genre = ?", (gentext,))
                        genid = db.fetchone()
                        if genid == None:
                            db.execute("INSERT INTO genres (genre) VALUES (?)", (gentext,))
                            conn.commit()
                            db.execute("SELECT id FROM genres WHERE genre = ?", (gentext,))
                            genid = db.fetchone()
                        gen3 = int(genid[0])
            db.execute(
                    "INSERT INTO titles (tconst, primaryTitle, " + 
                    "startYear, endYear, runtimeMinutes, genre1, genre2, genre3) " + 
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?);", 
                    (int(tconst[2:]), title, startY, endY, runtime, gen1, gen2, gen3))
            if count % step1 == 0:
                print("{:,} titles read...".format(count))
                conn.commit()
            if debug:
                if count >= debuglimit:
                    line = "---"
conn.commit()
print("Titles transfered; types and genres recorded...")

# transfer ratings
print("Transfering ratings...")
with open(folder+ratingsfile) as rfile:
    names = rfile.readline()[:-1].split("\t")
    line = "\n"
    count = 0
    while line[-1] == "\n":
        line = rfile.readline()
        if len(line) == 0:
            break
        #print(line)
        [tconst, rating, votes] = line.split("\t")
        if int(tconst[2:]) in titleset:
            count += 1
            votes = votes.strip("\n")
            db.execute(
                    "INSERT INTO ratings (tconst, averageRating, numVotes) " + 
                    "VALUES (?, ?, ?);", 
                    (int(tconst[2:]), float(rating), int(votes)))
            if count % step2 == 0:
                conn.commit()
                print("{:,} ratings read...".format(count))
            if debug:
                if count >= debuglimit:
                    line = "---"
conn.commit()
print("Ratings transfered...")

# transfer principal cast & crew
print("Transfering principal cast & crew...")
with open(folder+principalsfile) as pfile:
    names = pfile.readline()[:-1].split("\t")
    line = "\n"
    count = 0
    while line[-1] == "\n":
        line = pfile.readline()
        if len(line) == 0:
            break
        #print(line)
        [tconst, ordering, nconst, category, job, characters] = line.split("\t")
        if int(tconst[2:]) in titleset:
            count += 1
            peopleset.add(int(nconst[2:]))
            catid = "NULL"
            if category != "\\N":
                db.execute("SELECT id FROM categories WHERE category = ?", (category,))
                tempid = db.fetchone()
                if tempid == None:
                    db.execute("INSERT INTO categories (category) VALUES (?)", (category,))
                    conn.commit()
                    db.execute("SELECT id FROM categories WHERE category = ?", (category,))
                    tempid = db.fetchone()
                catid = int(tempid[0])
            db.execute(
                    "INSERT INTO principals (tconst, ordering, nconst, catid) " + 
                    "VALUES (?, ?, ?, ?);", 
                    (int(tconst[2:]), int(ordering), int(nconst[2:]), catid))
            if count % step5 == 0:
                print("{:,} cast & crew members read...".format(count))
                conn.commit()
            if debug:
                if count >= debuglimit:
                    line = "---"
conn.commit()
print("Principal cast & crew members transfered...")

# transfer people
print("Transfering people...")
with open(folder+namesfile) as nfile:
    names = nfile.readline()[:-1].split("\t")
    line = "\n"
    count = 0
    while line[-1] == "\n":
        line = nfile.readline()
        if len(line) == 0:
            break
        #print(line)
        [nconst, name, birthY, deathY, profs, knowns] = line.split("\t")
        if int(nconst[2:]) in peopleset:
            count += 1
            if birthY == "\\N":
                birthY = "NULL"
            else:
                birthY = int(birthY)
            if deathY == "\\N":
                deathY = "NULL"
            else:
                deathY = int(deathY)
            prof1 = "NULL"
            prof2 = "NULL"
            prof3 = "NULL"
            profs = profs.strip("\n")
            if profs != "\\N":
                profs = profs.split(",")
                proftext = profs[0]
                db.execute("SELECT id FROM professions WHERE profession = ?", (proftext,))
                profid = db.fetchone()
                if profid == None:
                    db.execute("INSERT INTO professions (profession) VALUES (?)", (proftext,))
                    conn.commit()
                    db.execute("SELECT id FROM professions WHERE profession = ?", (proftext,))
                    profid = db.fetchone()
                prof1 = int(profid[0])
                if len(profs) > 1:
                    proftext = profs[1]
                    db.execute("SELECT id FROM professions WHERE profession = ?", (proftext,))
                    profid = db.fetchone()
                    if profid == None:
                        db.execute("INSERT INTO professions (profession) VALUES (?)", (proftext,))
                        conn.commit()
                        db.execute("SELECT id FROM professions WHERE profession = ?", (proftext,))
                        profid = db.fetchone()
                    prof2 = int(profid[0])
                    if len(profs) > 2:
                        proftext = profs[2]
                        db.execute("SELECT id FROM professions WHERE profession = ?", (proftext,))
                        profid = db.fetchone()
                        if profid == None:
                            db.execute("INSERT INTO professions (profession) VALUES (?)", (proftext,))
                            conn.commit()
                            db.execute("SELECT id FROM professions WHERE profession = ?", (proftext,))
                            profid = db.fetchone()
                        prof3 = int(profid[0])
            db.execute(
                    "INSERT INTO people (nconst, primaryName, birthYear, deathYear, " + 
                    "prof1, prof2, prof3) " + 
                    "VALUES (?, ?, ?, ?, ?, ?, ?);", 
                    (int(nconst[2:]), name, birthY, deathY, prof1, prof2, prof3))
            if count % step3 == 0:
                conn.commit()
                print("{:,} names read...".format(count))
            if debug:
                if count >= debuglimit:
                    line = "---"
conn.commit()
print("People transfered...")

# transfer crew
print("Transfering directors and writers...")
with open(folder+crewfile) as cfile:
    names = cfile.readline()[:-1].split("\t")
    line = "\n"
    count = 0
    nextcount = step4
    while line[-1] == "\n":
        line = cfile.readline()
        if len(line) == 0:
            break
        #print(line)
        [tconst, directors, writers] = line.split("\t")
        if int(tconst[2:]) in titleset:
            writers = writers.strip("\n")
            if directors == "\\N":
                directors = []
            else:
                directors = directors.split(',')
            if writers == "\\N":
                writers = []
            else:
                writers = writers.split(',')
            for director in directors:
                count += 1
                db.execute(
                        "INSERT INTO directors (tconst, director) " + 
                        "VALUES (?, ?);", 
                        (int(tconst[2:]), int(director[2:])))
            for writer in writers:
                count += 1
                db.execute(
                        "INSERT INTO writers (tconst, writer) " + 
                        "VALUES (?, ?);", 
                        (int(tconst[2:]), int(writer[2:])))
            if count >= nextcount:
                conn.commit()
                print("{:,} directors and writers read...".format(nextcount))
                nextcount += step4
            if debug:
                if count >= debuglimit:
                    line = "---"
conn.commit()
print("Directors and writers transfered...")

del(titleset)
del(peopleset)
conn.close()