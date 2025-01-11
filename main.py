import json
import dbhandle
import shutil
import os





#set up directory
if not os.path.exists("games json"):
     os.path.exists("games json")
if not os.path.exists("games_db"):
     os.path.exists("games_db")


directory = "games json"
db = dbhandle.Amqdb()

#get list of files in dir games_json
fileindb = os.listdir("games_json")
for filename in os.listdir(directory):
        filepath = os.path.join(directory,filename)

        with open(filepath, "r") as game:
            game_dic = json.load(game)
            #check if game is in db it's done by the date and time of game so if game has the same date and time it will be ignored
            #since i only use this for me it shouldn't be a problem
            game_in_db = db.check_game(game_dic)

        if game_in_db is not None:
            print(game_in_db)
            print("the file {} is already in the db".format(filename))
        else:
            db.add_game(game_dic)
            db.get_song_type()
            #move file from from game_json to games_db
            shutil.move(filepath, r"games_db\{}".format(filename))
            print("the file {} added to the db".format(filename))

db.get_song_type()


