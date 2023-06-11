#!/usr/bin/env python3
import os
import sys
import logging
import eyed3
import sqlite3
from sqlite3 import Error

def getargs():
    if '-h' in sys.argv:
        print('{} [-d dbfile] [-m mp3directory]'.format(sys.argv[0]))
        print('''
        scans mp3directory and subdirectories for mp3 files and registers
        ID3-tag data in an sqlite3 database.
        
        If dbfilename is not specified it will default to mp3.db in the current directory.
        An existing DB will be expanded, otherwise a new db will be created.

        If mp3directory is not specified the current directory is used.
        ''')
        sys.exit()

    if '-m' in sys.argv:
        mp3dir = sys.argv[sys.argv.index('-m') + 1]
    else:
        mp3dir = os.getcwd()
    mp3dir = mp3dir.rstrip('/')
    logmsg('MP3 directory: {}'.format(mp3dir))

    if '-d' in sys.argv:
        mp3db = sys.argv[sys.argv.index('-d') + 1]
    else:
        mp3db = 'mp3.db'
    logmsg('MP3 database: {}'.format(mp3db))

    return mp3dir, mp3db

def logmsg(msg):
    logging.info(msg)
    print(msg)

def connectdb(mp3db):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    con = None
    try:
        con = sqlite3.connect(mp3db)
        return con
    except Error as e:
        logmsg(e)    
    return con

def createtable(con, tablesql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = con.cursor()
        c.execute(tablesql)
    except Error as e:
        logmsg(e)

def prepdb(mp3db):
    artists_table_sql = """ CREATE TABLE IF NOT EXISTS artists (
                         id integer PRIMARY KEY,
                         name text NOT NULL
                        ); """
    albums_table_sql = """ CREATE TABLE IF NOT EXISTS albums (
                        id integer PRIMARY KEY,
                        title text NOT NULL,
                        artist_id integer NOT NULL,
                        FOREIGN KEY (artist_id) REFERENCES artists (id)
                       ); """
    tracks_table_sql = """ CREATE TABLE IF NOT EXISTS tracks (
                        id integer PRIMARY KEY,
                        title text NOT NULL,
                        album_id integer NOT NULL,
                        artist_id integer NOT NULL,
                        tracknum integer,
                        bytes integer,
                        seconds integer,
                        disc text,
                        FOREIGN KEY (album_id) REFERENCES albums (id)
                        FOREIGN KEY (artist_id) REFERENCES artists (id)
                       ); """
    con = connectdb(mp3db)
    if con is not None:
        createtable(con, artists_table_sql)
        createtable(con, albums_table_sql)
        createtable(con, tracks_table_sql)
    else:
        logmsg("Error! cannot create the database connection.")
    return con

def dirwalk(con, dir):
    x = 0
    for root, dirs, files in os.walk(dir, topdown=True):
        for name in files:
            if '.' in name:
                if name.rsplit(sep='.', maxsplit=1)[1].upper() == 'MP3':
                    x += 1
                    mpf = eyed3.load(os.path.join(root, name))
                    processtrack(con, mpf, root, name)
                    if x % 1000 == 0:
                        logmsg('{} mp3 files processed'.format(x))
    logmsg('{} mp3 files processed'.format(x))
    return

def processtrack(con, mpf, root, name):
    if mpf is None:
        logmsg("===FOUT=== No ID3: {}".format(os.path.join(root, name)))
        return 
    if mpf.tag is None:
        logmsg("===FOUT=== No tag: {}".format(os.path.join(root, name)))
        return
# Get info from ID3tag
    artist = mpf.tag.artist   
    if artist is None:
        logmsg("===FOUT=== No artist, skipped: {}".format(os.path.join(root, name)))
        artist = 'Unknown'
        return
    else:
        artist = artist.strip()
    album = mpf.tag.album
    if album is None:
        logmsg("===FOUT=== No album, skipped: {}".format(os.path.join(root, name)))
        album = 'Unknown'
        return
    else:
        album = album.strip()
    albumartist = mpf.tag.album_artist
    if albumartist is None:
        logmsg("===FOUT=== No albumartist, skipped: {}".format(os.path.join(root, name)))
        albumartist = 'Unknown'
        return
    else:
        albumartist = albumartist.strip()
    track = mpf.tag.title
    if track is None:
        logmsg("===FOUT=== No tracktitle, skipped: {}".format(os.path.join(root, name)))
        track = 'Unknown'
        return
    else:
        track = track.strip()
        if len(track) == 0:
            logmsg("===FOUT=== Blank tracktitle, skipped: {}".format(os.path.join(root, name)))
            track = 'Unknown'
    tracknum = mpf.tag.track_num[0]
    if mpf.info is None:
        logmsg("===FOUT=== No info in ID3: {}".format(os.path.join(root, name)))
        return
    seconds = round(mpf.info.time_secs)
    bytes = mpf.info.size_bytes
    disc = finddiscname(root)
###tst   print('{} == {} == {} == {}'.format(disc, album, albumartist, artist))
# Create rows in db
# artist
    c = con.cursor()
    insert_artist_sql = ("INSERT INTO artists (name) "
                        "SELECT ? "
                        "WHERE NOT EXISTS (SELECT 1 FROM artists WHERE name = ?)")
    c.execute(insert_artist_sql, (artist, artist))
    c.execute(insert_artist_sql, (albumartist, albumartist))
# album
    albumartistid = c.execute("SELECT id FROM artists WHERE name = ? ;", (albumartist, )).fetchone()[0]
    insert_album_sql = ("INSERT INTO albums (title, artist_id) "
                        "SELECT ?, ? "
                        "WHERE NOT EXISTS (SELECT 1 FROM albums WHERE title = ? AND artist_id = ?)")
    c.execute(insert_album_sql, (album, albumartistid) * 2)
# track
    artistid = c.execute("SELECT id FROM artists WHERE name = ? ;", (artist, )).fetchone()[0]
    albumid = c.execute("SELECT id FROM albums WHERE title = ? AND artist_id = ?;", (album, albumartistid)).fetchone()[0]
    insert_track_sql = ("INSERT INTO tracks (title, album_id, artist_id, tracknum, bytes, seconds, disc) "
                        "SELECT ?, ?, ?, ?, ?, ?, ? "
                        "WHERE NOT EXISTS (SELECT 1 FROM tracks WHERE title = ? AND album_id = ? AND artist_id = ? AND disc = ?)")
    c.execute(insert_track_sql, (track, albumid, artistid, tracknum, bytes, seconds, disc, track, albumid, artistid, disc))

    return

def finddiscname(root):
    if 'MP3_V' in root:
        pos = root.index('MP3_V')
        disc = root[pos+5] + root[pos+9:pos+12]
        return disc
    if 'Top 2000 MP3' in root:
        if '0-10' in root:
            disc = 'T2K0'
            return disc
        pos = root.index('Top 2000 MP3')
        if '201' in root:    # 2016 of 2018
            disc = 'T2K' + root[pos+16]   # T2K6 of T2K8
            return disc
        disc = 'T2K' + root[pos+13]       # A-Z
        return disc
    return '0000'       # not a familiar path structure

def showcounts(con):
    c = con.cursor()
    artists = c.execute("SELECT COUNT(1) FROM artists ;").fetchone()[0]
    albums = c.execute("SELECT COUNT(1) FROM albums ;").fetchone()[0]
    tracks = c.execute("SELECT COUNT(1) FROM tracks ;").fetchone()[0]
    logmsg("Number of artists: {}, albums: {}, tracks: {}".format(artists, albums, tracks))

def main():
    logging.basicConfig(filename='mp3base.log', 
                        format='%(asctime)s %(levelname)s:%(message)s', 
                        level=logging.DEBUG)
    eyed3.log.setLevel("ERROR")
    mp3dir, mp3db = getargs()
    con = prepdb(mp3db)
    if con:
        logmsg("Counts at start:")
        showcounts(con)
        dirwalk(con, mp3dir)
        con.commit()
        logmsg("Counts at end:")
        showcounts(con)
        con.close()

if __name__ == "__main__":
    main()