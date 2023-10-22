import pylast
import os
import logging
import datetime 
from datetime import timezone, timedelta
import json
import sqlite3

logging.basicConfig(level=logging.INFO)

LASTFM_API_KEY = os.getenv('LASTFM_API_KEY')
LASTFM_SHARED_SECRET = os.getenv('LASTFM_SHARED_SECRET')
LASTFM_USERNAME = os.getenv('LASTFM_USERNAME')
LASTFM_PASSWORD_HASH = pylast.md5(os.getenv('LASTFM_PASSWORD'))
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
    dt = datetime.datetime.now(timezone.utc) 
    utc_time = dt.replace(tzinfo=timezone.utc) 
    utc_timestamp = str(int(utc_time.timestamp()))
    return utc_timestamp

def get_timestamp_minus_arg(daystoSubtract):
    dt = datetime.datetime.now(timezone.utc) 
    timestamp_to_subtract = timedelta(days=daystoSubtract)
    dt = dt - timestamp_to_subtract
    utc_time = dt.replace(tzinfo=timezone.utc) 
    utc_timestamp = str(int(utc_time.timestamp()))
    return utc_timestamp

def ticks_to_unix_timestamp(ticks):
    start = datetime.datetime(1, 1, 1)
    delta = datetime.timedelta(seconds=ticks/10000000)
    the_actual_date = start + delta
    return int(the_actual_date.timestamp())

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
    res = cur.execute("SELECT SongKey, Time FROM Listens ORDER BY Time asc")
    result = res.fetchall()
    con.close()

    result.sort(key=lambda x: x[1], reverse=False)

    for SongKey, Time in result:
        logs_key = str(SongKey) + "-" + str(Time)

        if logs_key in scrobbled_songs: 
            failed_songs.pop(logs_key, None)
            continue
        
        timestamp = int(ticks_to_unix_timestamp(Time))

        song = None

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




