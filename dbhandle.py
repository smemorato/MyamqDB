import mysql.connector
from mysql.connector import errorcode

import json


TABLES = {}
TABLES['games'] = ("""
    CREATE TABLE games(
      game_id INT NOT NULL AUTO_INCREMENT,
      room_name VARCHAR(255),
      start_time VARCHAR(255) UNIQUE,
      number_songs INT,
      PRIMARY KEY (game_id)
    ) ENGINE=InnoDB""")


TABLES['players'] = ("""
    CREATE TABLE players(
      player_id INT NOT NULL AUTO_INCREMENT,
      player_name VARCHAR(255) UNIQUE,
      PRIMARY KEY (player_id)
    ) ENGINE=InnoDB""")

TABLES['animes'] = (
    """CREATE TABLE animes (
    anime_id INT NOT NULL AUTO_INCREMENT,
    english_name VARCHAR(255),
    romaji_name VARCHAR(255),
    anime_type VARCHAR(255),
    season VARCHAR(6),
    year INT,
    anime_score TINYINT,
    amq_id INT UNIQUE,
    mal_id INT,
    kitsu_id INT,
    anilist_id INT,
    PRIMARY KEY (anime_id) 
    ) ENGINE=InnoDB""")

TABLES['tags'] = (
    """CREATE TABLE tags (
    tag_id INT NOT NULL AUTO_INCREMENT,
    tag VARCHAR(255),
    PRIMARY KEY (tag_id), UNIQUE KEY (tag) 
    ) ENGINE=InnoDB""")

TABLES['genres'] = (
    """CREATE TABLE genres (
    genre_id INT NOT NULL AUTO_INCREMENT,
    genre VARCHAR(255),
    PRIMARY KEY (genre_id), UNIQUE KEY (genre) 
    ) ENGINE=InnoDB""")

TABLES['tags_in_anime'] = (
    """CREATE TABLE tags_in_anime (
    anime_id INT,
    tag_id INT,
    PRIMARY KEY (anime_id,tag_id),
    FOREIGN KEY (anime_id) REFERENCES animes(anime_id),
    FOREIGN KEY (tag_id) REFERENCES tags(tag_id)

    ) ENGINE=InnoDB""")

TABLES['genres_in_anime'] = (
    """CREATE TABLE genres_in_anime (
    anime_id INT,
    genre_id INT,
    PRIMARY KEY (anime_id,genre_id),
    FOREIGN KEY (anime_id) REFERENCES animes(anime_id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
    ) ENGINE=InnoDB""")



# use 5 unique key because a song can be an OP and an ED and sometime there special versions  of songs that i assume 
# will have the same artist and name but diferent type_number
TABLES['songs'] = (
    """CREATE TABLE songs (
    song_id INT NOT NULL AUTO_INCREMENT,
    artist VARCHAR(255),
    song_name VARCHAR(255),
    anime_id INT ,
    song_type CHAR(2) ,
    type_number SMALLINT,
    anime_difficulty FLOAT(10),
    type_string VARCHAR(3),
    PRIMARY KEY (song_id),
    FOREIGN KEY (anime_id) REFERENCES animes(anime_id),
    CONSTRAINT unique_song UNIQUE(artist , song_name, song_type, type_number, anime_id)
    ) ENGINE=InnoDB""") #check how foat works




#weird table name, either i sould've made a songs_in_game and another table with the guessed or keep it like this but 
#name it differently
TABLES['songs_in_game'] = ("""
    CREATE TABLE songs_in_game (
    game_id INT,
    song_id INT,
    player_id INT,
    guessed_correct BOOLEAN,
    in_list BOOLEAN,
    score INT,
    PRIMARY KEY (game_id, song_id, player_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (song_id) REFERENCES songs(song_id),
    FOREIGN KEY (player_id) REFERENCES players(player_id)
    )""")




class Amqdb():
    def __init__(self):
        with open(r"configdb.JSON", "r") as file:
            config = json.load(file)


        DB_NAME = "myamq"

        # connect to mysql
        try:
            self.cnx = mysql.connector.connect(**config)
            self.cursor = self.cnx.cursor()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

        # Create db
        try:
            self.cursor.execute("USE {}".format(DB_NAME))
        except mysql.connector.Error as err:
            print("Database {} does not exists.".format(DB_NAME))
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                try:
                    self.cursor.execute(
                    "CREATE DATABASE {} DEFAULT CHARACTER SET 'UTF8MB4'".format(DB_NAME))
                    
                    print("Database {} created successfully.".format(DB_NAME))
                except mysql.connector.Error as err:
                    print("Failed creating database: {}".format(err))
                    exit(1)


                self.cnx.database = DB_NAME
            else:
                print(err)
                exit(1)

        #create tables
        for table_name in TABLES:
            table_description = TABLES[table_name]
            try:
                print("Creating table {}: ".format(table_name), end='')
                self.cursor.execute(table_description)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)
            else:
                print("OK")
                

    #if there are two games with the same start time and date it will ignore the second 
    #but since i only use this for myself this is fine
    def check_game(self, game):
                
        self.cursor.execute("""SELECT * FROM games 
                            WHERE start_time ="{}"
                            """.format(game["startTime"]))
        game_in_db = self.cursor.fetchone()

        
        return game_in_db

    
    def add_game(self,game):

            # this for loop is supposed to list the players in game by checking every answer and song origin. Since the json doesn't list the player
            # however if the game doesn't play any song from a player's list and this player doesn't guessed any song it won't be listed anywhere
            # i could add my username directly to the list since i probably will only add my games
            playerlist = []
            for song in game["songs"]:
                
                playerlist.extend([item for item in song["correctGuessPlayers"] if item not in playerlist])
                for plist in song["listStates"]:
                    if plist["name"] not in  playerlist:
                        playerlist.append(plist["name"])



            add_game_query = ("""INSERT INTO games
                            (room_name, start_time,number_songs)
                            VALUES (%s, %s, %s)""")
            #game["songs"][-1]["songNumber"] gets the number of the las song
            game_data = (game["roomName"], game["startTime"],game["songs"][-1]["songNumber"])
            self.cursor.execute(add_game_query, game_data)

            self.cursor.execute("SELECT LAST_INSERT_ID()")
            game_id = self.cursor.fetchall()



            for player in playerlist:
                add_player =("""INSERT INTO players (player_name) 
                            VALUES ("{}")
                            ON DUPLICATE KEY UPDATE
                            player_id = player_id""".format(player))
                
                self.cursor.execute(add_player)

            
            for song in game["songs"]:
                songinfo = song["songInfo"]
                
                #remove ' from the name name of title
                if songinfo["animeNames"]["english"].count("'")>=1:
                    songinfo["animeNames"]["english"]=songinfo["animeNames"]["english"].replace("'","''")

                
                anime_data = {'english_name':songinfo["animeNames"]["english"],
                            'romaji_name':songinfo["animeNames"]["romaji"],
                            'anime_type':songinfo["animeType"],
                            'season':songinfo["vintage"][:len(songinfo["vintage"])-5],
                            'year':int(songinfo["vintage"][len(songinfo["vintage"])-4:]),
                            'anime_score':songinfo["animeScore"],
                            'amq_id':songinfo["annId"],
                            'mal_id': songinfo["siteIds"]["malId"],
                            'kitsu_id': songinfo["siteIds"]["kitsuId"],
                            'anilist_id':songinfo["siteIds"]["aniListId"],
                            'nenglish_name':songinfo["animeNames"]["english"],
                            'nromaji_name':songinfo["animeNames"]["romaji"],
                            'ntype':songinfo["animeType"],
                            'nseason':songinfo["vintage"][:len(songinfo["vintage"])-5],
                            'nyear':int(songinfo["vintage"][len(songinfo["vintage"])-4:]),
                            'anime_score':songinfo["animeScore"],
                            'nmal_id': songinfo["siteIds"]["malId"],
                            'nkitsu_id': songinfo["siteIds"]["kitsuId"],
                            'nanilist_id':songinfo["siteIds"]["aniListId"]}

                # **operator return argument arguments as a par of key and variable
                add_anime = ("""INSERT INTO animes
                            (english_name, romaji_name, anime_type, season, year, anime_score, amq_id, mal_id, kitsu_id, anilist_id)
                            VALUES 
                            ('{english_name}', "{romaji_name}", "{anime_type}", "{season}", {year}, {anime_score}, 
                            {amq_id}, {mal_id}, {kitsu_id}, {anilist_id})
                            ON DUPLICATE KEY UPDATE
                            english_name = '{nenglish_name}',
                            romaji_name = "{nromaji_name}",
                            anime_type = "{ntype}",
                            season = "{nseason}",
                            year = {nyear},
                            anime_score = {anime_score},
                            mal_id = {nmal_id},
                            kitsu_id = {nkitsu_id},
                            anilist_id = {nanilist_id}""").format(**anime_data)
                

                print(add_anime)
                
                self.cursor.execute(add_anime)

                # for whatevere reason INSERT IGNORE doesn't ignore
                for tag in songinfo["animeTags"]:
                    add_tag = ("""INSERT INTO tags
                                (tag)
                                VALUES ("{}")
                                ON DUPLICATE KEY UPDATE
                                tag_id=tag_id
                                """.format(tag))
                
                    

                    self.cursor.execute(add_tag)

                    add_tag_to_anime = ("""INSERT INTO tags_in_anime 
                                        (anime_id, tag_id) 
                                        VALUES (
                                        (SELECT  anime_id FROM animes WHERE amq_id = {}),
                                        (SELECT tag_id FROM tags  WHERE tag= "{}"))
                                        ON DUPLICATE KEY UPDATE
                                        tag_id=tag_id""".format(songinfo["annId"],tag))

                    self.cursor.execute(add_tag_to_anime)

                    

                for genre in songinfo["animeGenre"]:
                    add_genre = ("""INSERT INTO genres
                                (genre)
                                VALUES (%s)
                            ON DUPLICATE KEY UPDATE
                            genre_id=genre_id
                                """)
                
                    genre_data = ([genre])
                    self.cursor.execute(add_genre,genre_data)



                    add_genre_to_anime = ("""INSERT INTO genres_in_anime 
                                        (anime_id, genre_id) 
                                        VALUES (
                                        (SELECT  anime_id FROM animes WHERE amq_id = {}),
                                        (SELECT genre_id FROM genres  WHERE genre= "{}"))
                                        ON DUPLICATE KEY UPDATE
                                        genre_id=genre_id""".format(songinfo["annId"],genre))

                    self.cursor.execute(add_genre_to_anime)
                
                add_song = ("""INSERT INTO songs
                            (artist, song_name, anime_id, song_type, type_number, anime_difficulty)
                            VALUES(
                            "{artist}",
                            "{song_name}",
                            (SELECT  anime_id FROM animes WHERE amq_id = {animeid}),
                            {song_type},
                            {type_number},
                            {anime_difficulty}
                            )
                            ON DUPLICATE KEY UPDATE
                            anime_difficulty = {nanime_difficulty}
                            """.format(artist = songinfo["artist"],
                                    song_name = songinfo["songName"],
                                    animeid = songinfo["annId"],
                                    song_type = songinfo["type"],
                                    type_number = songinfo["typeNumber"],
                                    anime_difficulty = songinfo["animeDifficulty"],
                                    nanime_difficulty = songinfo["animeDifficulty"]))

                self.cursor.execute(add_song)


                for player in playerlist:

                    list =False
                    guesscorrect = False
                    aniscore = "NULL"
                    i = 0
                    if player in song["correctGuessPlayers"]:
                        guesscorrect = True

                    while list == False and i< len(song["listStates"]):
                        if song["listStates"][i]["name"] == player:
                            list = True
                            aniscore = song["listStates"][i]["score"]
                        i+=1

                    #TODO attribuite a number to the Insert songs otherwise i would be able to group 
                    add_song_to_game = ("""INSERT INTO songs_in_game
                                        (game_id, song_id, player_id, guessed_correct, in_list,score)
                                        VALUES(
                                        (SELECT game_id FROM games WHERE start_time = "{game_time}"),
                                        (SELECT song_id FROM songs 
                                            WHERE 
                                                artist = "{artist}" AND 
                                                song_name = "{song_name}" AND 
                                                anime_id = (SELECT  anime_id FROM animes WHERE amq_id = {amqid}) AND
                                                song_type = {song_type} AND
                                                type_number = {type_number}),
                                        (SELECT player_id FROM players WHERE player_name = "{player_name}"),
                                        {guessed_correct},
                                        {in_list},
                                        {score}
                                        )""".format(
                                            game_time = game["startTime"],
                                            artist = songinfo["artist"],
                                            song_name = songinfo["songName"],
                                            amqid = songinfo["annId"],
                                            song_type = songinfo["type"],
                                            type_number = songinfo["typeNumber"],
                                            player_name = player,
                                            guessed_correct = guesscorrect,
                                            in_list = list,
                                            score = aniscore  
                                            ))

                    self.cursor.execute(add_song_to_game)
            self.cnx.commit()       
            print("game {} added to db".format(game["startTime"]))


    def get_song_type(self):

        # add column for the types just to be easier to see 
        self.cursor.execute("""UPDATE songs 
                        SET type_string = "OP" 
                        WHERE song_type = 1""")
        self.cursor.execute("""UPDATE songs 
                        SET type_string = "ED" 
                        WHERE song_type = 2""")
        self.cursor.execute("""UPDATE songs 
                        SET type_string = "IN" 
                        WHERE song_type = 3""")
        self.cnx.commit()


        

    


        





