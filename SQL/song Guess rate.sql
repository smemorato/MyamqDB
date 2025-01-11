use myamq;

Select 
	english_name,
	song_type,
	type_number,
    anime_score,
	sum(guessed_correct) AS "times_guessed_correctly",
	COUNT(english_name) AS "times_guessed",
	AVG(guessed_correct)*100 AS Percentage,
    anime_difficulty,
    ROUND((AVG(guessed_correct)*100 - anime_difficulty),2) AS "difficulty_difference"
    
    
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
songs.anime_id,
song_type,
type_number,
anime_difficulty
