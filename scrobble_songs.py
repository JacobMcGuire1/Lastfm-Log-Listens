import pylast
import os
import logging
import datetime 
from datetime import timezone, timedelta, datetime
import json
import sqlite3

import pytz

logging.basicConfig(level=logging.INFO)

auth_data = {}

auth_data_file = "./authinfo.json"
if os.path.isfile(auth_data_file):
    with open(auth_data_file) as f:
        auth_data = json.loads(f.read())

LASTFM_API_KEY = auth_data["LASTFM_API_KEY"]#os.getenv('LASTFM_API_KEY')
LASTFM_SHARED_SECRET = auth_data["LASTFM_SHARED_SECRET"]#os.getenv('LASTFM_SHARED_SECRET')
LASTFM_USERNAME = auth_data["LASTFM_USERNAME"]#os.getenv('LASTFM_USERNAME')
LASTFM_PASSWORD_HASH = auth_data["LASTFM_PASSWORD_HASH"]#pylast.md5(os.getenv('LASTFM_PASSWORD'))
SESSION_KEY_FILE = "session_key"


progress_folder = "data"

def authenticate():
    network = pylast.LastFMNetwork(LASTFM_API_KEY, LASTFM_SHARED_SECRET)
    if not os.path.exists(SESSION_KEY_FILE):
        skg = pylast.SessionKeyGenerator(network)
        url = skg.get_web_auth_url()

        print(f"Please authorize this script to access your account: {url}\n")
        import time
        import webbrowser

        webbrowser.open(url)

        while True:
            try:
                session_key = skg.get_web_auth_session_key(url)
                with open(SESSION_KEY_FILE, "w") as f:
                    f.write(session_key)
                break
            except pylast.WSError:
                time.sleep(1)
    else:
        session_key = open(SESSION_KEY_FILE).read()

    network.session_key = session_key

    return network

def get_current_timestamp():
    dt = datetime.now(timezone.utc) 
    utc_time = dt.replace(tzinfo=timezone.utc) 
    utc_timestamp = str(int(utc_time.timestamp()))
    return utc_timestamp

def get_timestamp_minus_arg(daystoSubtract):
    dt = datetime.now(timezone.utc) 
    timestamp_to_subtract = timedelta(days=daystoSubtract)
    dt = dt - timestamp_to_subtract
    utc_time = dt.replace(tzinfo=timezone.utc) 
    utc_timestamp = str(int(utc_time.timestamp()))
    return utc_timestamp

def ticks_to_unix_timestamp(ticks):
    start = datetime(1, 1, 1)
    delta = timedelta(seconds=ticks/10000000)
    the_actual_date = start + delta
    return int(the_actual_date.timestamp())

def transform_artist(artist):
    match artist:
        case "Chaos Chaos (formerly Smoosh)":
            return "Chaos Chaos"
        case _:
            return artist
        
def transform_songkey(songkey):
    if "SChaos Chaos (formerly Smoosh)" in songkey:
        songkey = songkey.replace("SChaos Chaos (formerly Smoosh)", "Chaos Chaos")
    else:
        songkey = songkey.replace("Chaos Chaos (formerly Smoosh)", "Chaos Chaos")
    songkey = songkey.replace("Flaming Pie", "Flaming Pie (Archive Collection)")
    songkey = songkey.replace("Kisses On The Bottom", "Kisses On The Bottom - Complete Kisses")
    return songkey

def get_current_datetime_string():
    now = datetime.now()
    return now.strftime("%d/%m/%Y %H:%M:%S")

print("Starting run at: " + get_current_datetime_string())

network = authenticate()

surfaceold = "./surface/Surface Song Data Older (done)"
surfacenew = "./surface/surface song data newer"
ybbinold = "./ybbinpc pre win11/older one"
ybbinnew = "./ybbinpc pre win11/newer (large)"

folders = [surfaceold, surfacenew, ybbinold, ybbinnew]

failed_in_run = 0
succeeded_in_run = 0
not_found_in_dict = 0

scrobbled_songs = {}
failed_songs = {}

scrobbled_songs_file = progress_folder + "/scrobbled_songs.json"
if os.path.isfile(scrobbled_songs_file):
    with open(scrobbled_songs_file) as f:
        scrobbled_songs = json.loads(f.read())

failed_songs_file = progress_folder + "/failed_songs.json"
if os.path.isfile(failed_songs_file):
    with open(failed_songs_file) as f:
        failed_songs = json.loads(f.read())

for import_data_folder in folders:
    songdict = {}
    songdictfile = import_data_folder + "/songdict.txt"
    if os.path.isfile(songdictfile):
        with open(songdictfile, encoding="utf8") as f:
            songdict = json.loads(f.read())

    con = sqlite3.connect(import_data_folder + "/songlog.db")
    cur = con.cursor()

    columns = [i[1] for i in cur.execute('PRAGMA table_info(Listens)')]

    hasListens = True

    if ("Listens" in columns):
        res = cur.execute("SELECT SongKey, Time, Logged FROM Listens ORDER BY Time asc")
    else:
        hasListens = False
        res = cur.execute("SELECT SongKey, Time FROM Listens ORDER BY Time asc")
    
    result = res.fetchall()
    con.close()

    if not hasListens:
        result = [ (*row, '0') for row in result]

    result.sort(key=lambda x: x[1], reverse=False)

    for SongKey, Time, Logged in result:
        
        if Logged == 1:
            continue

        logs_key = str(SongKey) + "-" + str(Time)

        if logs_key in scrobbled_songs: 
            failed_songs.pop(logs_key, None)
            continue
        
        timestamp = int(ticks_to_unix_timestamp(Time))

        song = None

        if (SongKey in songdict):
            song = songdict[SongKey]
        else:
            SongKey = transform_songkey(SongKey)
            if (SongKey in songdict):
                song = songdict[SongKey]
            else:
                not_found_in_dict += 1
                message = "Key not found in songdict (not a lastfm error)"
                print("Failure: " + SongKey)
                print("Error Message: " + message + "\n")
                if (logs_key in failed_songs):
                    failed_songs[logs_key]["ExceptionMessage"].append(message)
                else:
                    failed_songs[logs_key] = { "SongKey": SongKey, "Time": Time, "ExceptionMessage": [message] }
                continue

        artist = song["Artist"]
        title = song["Title"]
        album = song["Artist"]
        
        two_weeks_ago = get_timestamp_minus_arg(13)
        response = None

        print(f"Songkey: {SongKey}, Time in ticks: {Time}, UTC Timestamp: {timestamp}")
        if (logs_key in failed_songs):
            failurecount = len(failed_songs[logs_key]["ExceptionMessage"])
            if (failurecount > 3):
                print("Skipping due to too many failures: " + logs_key)
        try:
            response = network.scrobble(artist=artist, title=title, album=album, timestamp=two_weeks_ago)
            scrobbled_songs[logs_key] = { "SongKey": SongKey, "Time": Time }
            failed_songs.pop(logs_key, None)
            succeeded_in_run += 1
        except Exception as ex:
            failed_in_run += 1
            message = str(ex)
            print("Failure: " + SongKey)
            print("Error Message: " + message + "\n")
            if (logs_key in failed_songs):
                failed_songs[logs_key]["ExceptionMessage"].append(message)
            else:
                failed_songs[logs_key] = { "SongKey": SongKey, "Time": Time, "ExceptionMessage": [message] }
        
        if failed_in_run > 10: 
            print("Stopping due to more than 10 failures.")
            break
    
print("Successes: " + str(succeeded_in_run))
print("Failures: " + str(failed_in_run))
print("Not Found In Dict: " + str(not_found_in_dict))

with open(scrobbled_songs_file, 'w',  encoding="utf8") as f:
        json.dump(scrobbled_songs,f)

with open(failed_songs_file, 'w',  encoding="utf8") as f:
        json.dump(failed_songs,f)

print("Run finished at: " + get_current_datetime_string())




