use myamq;




SELECT 
animes.year,
COUNT(songs_in_game.song_id) AS nb_guesses,
COUNT(DISTINCT(songs_in_game.song_id)) AS nb_shows,
AVG(guessed_correct)*100 AS Percentage
FROM songs_in_game
INNER JOIN songs
	ON songs_in_game.song_id = songs.song_id
INNER JOIN animes
	ON animes.anime_id = songs.anime_id
INNER JOIN players
	ON songs_in_game.player_id = players.player_id
WHERE
	player_name = "bob"
GROUP BY
animes.year
ORDER BY
animes.year DESC
