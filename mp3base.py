#!/usr/bin/env python3

###TODOOOOO   main() weer invoeren?   &    path

import os
import sys
import logging
import eyed3
import sqlite3
from sqlite3 import Error
import difflib

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

def create_db_object(con, tablesql):
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
                         name text NOT NULL UNIQUE

                        ); """
    albums_table_sql = """ CREATE TABLE IF NOT EXISTS albums (
                        id integer PRIMARY KEY,
                        title text NOT NULL,
                        artist_id integer NOT NULL,
                        UNIQUE (title, artist_id),
                        FOREIGN KEY (artist_id) REFERENCES artists (id)
                       ); """
    album_feat_table_sql = """ CREATE TABLE IF NOT EXISTS album_feat (
                            album_id integer NOT NULL,
                            artist_id integer NOT NULL,
                            UNIQUE (album_id, artist_id),
                            FOREIGN KEY (album_id) REFERENCES albums (id)
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
                        path text,
                        file text,
                        UNIQUE(title, album_id, artist_id, disc),
                        FOREIGN KEY (album_id) REFERENCES albums (id)
                        FOREIGN KEY (artist_id) REFERENCES artists (id)
                       ); """
    track_feat_table_sql = """ CREATE TABLE IF NOT EXISTS track_feat (
                            track_id integer NOT NULL,
                            artist_id integer NOT NULL,
                            UNIQUE (track_id, artist_id),
                            FOREIGN KEY (track_id) REFERENCES tracks (id)
                            FOREIGN KEY (artist_id) REFERENCES artists (id)
                           ); """
    con = connectdb(mp3db)
    if con is not None:
        create_db_object(con, artists_table_sql)
        create_db_object(con, albums_table_sql)
        create_db_object(con, album_feat_table_sql)
        create_db_object(con, tracks_table_sql)
        create_db_object(con, track_feat_table_sql)
    else:
        logmsg("Error! cannot create the database connection.")
    return con

def dirwalk(con, dir):
    x = 0
    create_artist_dict(con)
    for root, dirs, files in os.walk(dir, topdown=True):
        for name in files:
            if '.' in name:
                if name.rsplit(sep='.', maxsplit=1)[1].upper() == 'MP3':
                    processtrack(con, root, name)
                    x += 1
                    print('\n\n'+'='*15+'>>> Track {} <<<'.format(x)+'='*15)
    logmsg('{} mp3 files processed'.format(x))
    return

def create_artist_dict(con):
    global ARTIST_DICT
# get artist names with ids
    c = con.cursor()
    artists = c.execute("SELECT name, id FROM artists;").fetchall()
    logmsg('{} artists written to dictionary'.format(len(artists)))
    ARTIST_DICT = dict(artists)

def processtrack(con, root, name):
    print("\n"+("-"*40))
    logmsg("Root: {}".format(root))
    logmsg("File: {}".format(name))
    mpf = eyed3.load(os.path.join(root, name))
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
    logmsg("****    Trackartist: {}    ****".format(artist))
    album = mpf.tag.album
    if album is None:
        logmsg("===FOUT=== No album, skipped: {}".format(os.path.join(root, name)))
        album = 'Unknown'
        return
    else:
        album = album.strip()
    logmsg("****    Album: {}    ****".format(album))
    albumartist = mpf.tag.album_artist
    if albumartist is None:
        logmsg("===FOUT=== No albumartist, skipped: {}".format(os.path.join(root, name)))
        albumartist = 'Unknown'
        return
    else:
        albumartist = albumartist.strip()
    logmsg("****    Albumartist: {}    ****".format(albumartist))
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
    logmsg("****    Tracktitle: {}    ****".format(track))
    tracknum = mpf.tag.track_num[0]
    if mpf.info is None:
        logmsg("===FOUT=== No info in ID3: {}".format(os.path.join(root, name)))
        return
    seconds = round(mpf.info.time_secs)
    bytes = mpf.info.size_bytes
    disc, path = finddiscpath(root)
    logmsg("Disc: {}".format(disc))
    logmsg("Path: {}".format(path))

# Create rows in db
# First split artists and featuring artists.
# And when artistname is not yet in the database, give naming suggestion from
# existing artists and let the user enter his choice (sometimes by cut'n'paste)
# of one of the suggestions

    artist, track_featuring = unfeat_artist(artist)
    artist = insert_artist(artist, con)
    track_feat_corrected = []
    for featuring_artist in track_featuring:
        featuring_artist = insert_artist(featuring_artist, con)
        track_feat_corrected.append(featuring_artist)
    albumartist, album_featuring = unfeat_artist(albumartist)
    albumartist = insert_artist(albumartist, con)
    album_feat_corrected = []
    for featuring_artist in album_featuring:
        featuring_artist = insert_artist(featuring_artist, con)
        album_feat_corrected.append(featuring_artist)
    c = con.cursor()
# album
    albumartistid = ARTIST_DICT[albumartist]
    insert_album_sql = ("INSERT INTO albums (title, artist_id) "
                        "SELECT ?, ? "
                        "WHERE NOT EXISTS (SELECT 1 FROM albums WHERE title = ? AND artist_id = ?)")
    c.execute(insert_album_sql, (album, albumartistid, album, albumartistid))
    albumid = c.execute("SELECT id FROM albums WHERE title = ? AND artist_id = ?;", (album, albumartistid)).fetchone()[0]

## Add NtoN relations between album and featuring artists
    insert_album_feat = ("INSERT INTO album_feat (album_id, artist_id) "
                         "SELECT ?, ? "
                         "WHERE NOT EXISTS (SELECT 1 FROM album_feat WHERE album_id = ? AND artist_id = ?)")
    for f_artist in album_feat_corrected:
        c.execute(insert_album_feat, (albumid, ARTIST_DICT[f_artist], albumid, ARTIST_DICT[f_artist])) 
# track
    artistid = ARTIST_DICT[artist]
    insert_track_sql = ("INSERT INTO tracks (title, album_id, artist_id, tracknum, bytes, seconds, disc, path, file) "
                        "SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?"
                        "WHERE NOT EXISTS (SELECT 1 FROM tracks WHERE title = ? AND album_id = ? AND artist_id = ? AND disc = ?)")
    c.execute(insert_track_sql, (track, albumid, artistid, tracknum, bytes, seconds, disc, path, name, track, albumid, artistid, disc))
    trackid = c.execute("SELECT id FROM tracks WHERE title = ? AND album_id = ? AND artist_id = ? AND disc = ?;",
                        (track, albumid, artistid, disc)).fetchone()[0]
## Add NtoN relations between track and featuring artists
    insert_track_feat = ("INSERT INTO track_feat (track_id, artist_id) "
                         "SELECT ?, ? "
                         "WHERE NOT EXISTS (SELECT 1 FROM track_feat WHERE track_id = ? AND artist_id = ?)")
    for f_artist in track_feat_corrected:
        c.execute(insert_track_feat, (trackid, ARTIST_DICT[f_artist], trackid, ARTIST_DICT[f_artist])) 
    con.commit()        # Commit on each processed track
    return

def finddiscpath(root):
    if 'MP3_V' in root:
        pos = root.index('MP3_V')
        disc = root[pos+5] + root[pos+9:pos+12]
        path = root[pos+13:]
        return disc, path
    if 'Top 2000 MP3' in root:
        pos = root.index('Top 2000 MP3')
        path = root[pos+13:]
        if '0-10' in root:
            disc = 'T2K0'
            return disc, path
        if '201' in root:    # 2016 of 2018
            disc = 'T2K' + root[pos+16]   # T2K6 of T2K8
            return disc, path
        disc = 'T2K' + root[pos+13]       # A-Z
        return disc, path
    return '0000', '/'       # not a familiar path structure

def unfeat_artist(artist):
    global LAST_FEATURED_ARTIST
    global LAST_UNFEATURED_ARTIST
    global LAST_FEATURINGS
# Unfeat artist, which means: separate artist from featuring artist(s)
# Routine to split artist from featuring artists and featuring artists from
# each other. Done with dialog.
    L = []  #Start with assumption of no featuring artists
    if artist.lower().startswith('the '):
        artist = artist[4:]
    if artist.lower().startswith('de '): #Dutch 'the'
        artist = artist[3:]
    if artist.lower().endswith(', the'):
        artist = artist[:-5]
    if artist in ARTIST_DICT:
            return artist, L
    if artist == LAST_FEATURED_ARTIST:  # Don't repeat old 'splitting' dialog but reuse
        return LAST_UNFEATURED_ARTIST, LAST_FEATURINGS
    
    print('Artist in album/track:')
    print('>>> {} <<<'.format(artist))
# Split automated?
    if ('feat' in artist.lower() or 
        ' ft' in artist.lower() or
        ' with ' in artist.lower() or
        ' and ' in artist.lower() or
        ' guest ' in artist.lower() or
        '&' in artist.lower() or
        '+' in artist.lower() or
        ',' in artist.lower()
       ):
        as_artist, as_L = autosplit(artist)

        ## if all artists in suggested split are known, the split is probably correct
        if as_artist in ARTIST_DICT and set(as_L).issubset(set(ARTIST_DICT.keys())):
            return as_artist, as_L
        ## else ask user
        print("\n\nEnter 'u' to use suggested split (You'll be able to correct individual typo's later)")
        print("Not entering 'u' will lead you into the manual splitting process")
        answer = input().lower()
        if answer == "u":
            LAST_FEATURED_ARTIST = artist
            LAST_UNFEATURED_ARTIST = as_artist
            LAST_FEATURINGS = as_L[:]
            return as_artist, as_L

    answer = input('\nSplit in artist and featurings? y/n: ').lower()
    if not answer == 'y':
        return artist, L
    
    LAST_FEATURED_ARTIST = artist

# Split manually    
    artist = input('\nEnter artist without "Featuring Artists": ')
    LAST_UNFEATURED_ARTIST = artist
    while True:
        print('Enter one featuring artist name')
        answer = input('>>>> OR "d" for done: ')
        if answer == 'd' or answer == 'D':
            break
        L.append(answer)
    LAST_FEATURINGS = L[:] # create a copy
    return artist, L

def autosplit(artist):
    L = artist.split('Featuring')
    L3 = []
    for part in L:
        L2 = part.split('featuring')
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split('FEATURING')
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split('FEAT ')
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split('Feat ')
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split('feat ')
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split('feat.')
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split('Feat.')
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split("FEAT.")
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split(" ft.")
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split(" ft ")
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split(" Ft.")
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split("&")
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split("+")
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split(",")
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split(" and ")
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split(" AND ")
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split(" And ")
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split(" with ")
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split(" WITH ")
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split(" guest ")
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split(" Guest ")
        L3.extend(L2)
    L = L3[:]
    L3 = []
    for part in L:
        L2 = part.split(" GUEST ")
        L3.extend(L2)
    L2 = [part.strip() for part in L3]
    L = []
    for i in range(len(L2)):
        if L2[i].lower().startswith('the '):
            L.append(L2[i][4:])
        elif L2[i].lower().startswith('de '):
            L.append(L2[i][3:])
        else:
            L.append(L2[i])

    print("\n**Split suggestion**")
    print("\nMain artist:")
    print("\t"+L[0])
    print("Featuring artist(s):")
    for feat_art in L[1:]:
        print("\t"+feat_art)
    return L[0], L[1:]

def insert_artist(artist, con):
    global ARTIST_DICT
    if artist.lower().startswith('the '):
        artist = artist[4:]
    if artist.lower().startswith('de '): #Dutch 'the'
        artist = artist[3:]
    if artist in ARTIST_DICT: # artist already in db
        return artist
    artist = correct_artist(artist) # Check if it is really a new artist or a typo
    if artist in ARTIST_DICT: # artist already in db
        return artist
    c = con.cursor()
    insert_artist_sql = ("INSERT INTO artists (name) "
                        "SELECT ? "
                        "WHERE NOT EXISTS (SELECT 1 FROM artists WHERE name = ?)")
    c.execute(insert_artist_sql, (artist, artist))
    artistid = c.execute("SELECT id FROM artists WHERE name = ? ;", (artist, )).fetchone()[0]
    ARTIST_DICT[artist] = artistid
    return artist

def correct_artist(artist):
    """    Case insensitive close match
    def get_close_matches_icase(word, possibilities, *args, **kwargs):
    --- Case-insensitive version of difflib.get_close_matches ---
    lword = word.lower()
    lpos = {}
    for p in possibilities:
        if p.lower() not in lpos:
            lpos[p.lower()] = [p]
        else:
            lpos[p.lower()].append(p)
    lmatches = difflib.get_close_matches(lword, lpos.keys(), *args, **kwargs)
    ret = [lpos[m] for m in lmatches]
    ret = itertools.chain.from_iterable(ret)
    return set(ret)
    """
    print('Artist: {}'.format(artist))
    print('\n!!! No artist with this exact name found in the database.')
    like_list = difflib.get_close_matches(artist, list(ARTIST_DICT), n=5, cutoff=0.7)
    print('     Do you want to: (Enter the letter in brackets to choose)')
    print('     (u) Use {}'.format(artist))
    if len(like_list) > 0:
        print('Suggestions from existing artists:')
        x = 0
        for l_artist in like_list:
            print('     ({}) Use {}'.format(chr(x+97), l_artist))
            x = x + 1
    print('  or (t) Type the artistname')
    keuze = input().lower()
    if keuze == 'u':
        return artist
    if keuze == 't':
        artist = input('Enter artist name: ')
        return artist
    while True:
        if len(keuze) != 1 or ord(keuze) < 97 or ord(keuze) > 96+len(like_list):
            print('Wrong choice, use one of the suggested letters for the alternatives.')
            print('(or you might use "u" to use the default artist)')
            keuze = input().lower()
        else:
            break
        if keuze == 'u':
            return(artist)
    return(like_list[ord(keuze)-97])

def showcounts(con):
    c = con.cursor()
    artists = c.execute("SELECT COUNT(1) FROM artists ;").fetchone()[0]
    albums = c.execute("SELECT COUNT(1) FROM albums ;").fetchone()[0]
    tracks = c.execute("SELECT COUNT(1) FROM tracks ;").fetchone()[0]
    logmsg("Number of artists: {}, albums: {}, tracks: {}".format(artists, albums, tracks))

ARTIST_DICT = {}
LAST_FEATURED_ARTIST = ''
LAST_UNFEATURED_ARTIST = ''
LAST_FEATURINGS = []

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