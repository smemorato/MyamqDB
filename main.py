import json
import dbhandle
import shutil
import os

directory = "games json"
db = dbhandle.Amqdb()
fileindb = os.listdir("games_db")
for filename in os.listdir(directory):
        filepath = os.path.join(directory,filename)

        with open(filepath, "r") as game:
            game_dic = json.load(game)

            game_in_db = db.check_game(game_dic)

        if game_in_db is not None:
            print(game_in_db)
            game_in_db = True
            print("the file {} is already in the db".format(filename))
        else:
            game_in_db = False

            db.add_game(game_dic)
            db.get_song_type()
            shutil.move(filepath, r"games_db\{}".format(filename))
            print("the file {} added to the db".format(filename))

db.get_song_type()


