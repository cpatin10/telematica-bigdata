registros = LOAD '/user/cpatin10/datasets/peliculas.csv' USING PigStorage(',') AS (user_id:int, movie_id:int, rating:float, genero:chararray, fecha:chararray);
agrupacion_peliculas = GROUP registros BY movie_id;
peliculas_usuario = FOREACH agrupacion_peliculas GENERATE group, (COUNT(registros), AVG(registros.rating));

DUMP peliculas_usuario;
STORE peliculas_usuario INTO '/user/cpatin10/pig/outUsuariosXPeliculas' USING PigStorage (',');
