registros = LOAD '/user/cpatin10/datasets/peliculas.csv' USING PigStorage(',') AS (user_id:int, movie_id:int, rating:float, genero:chararray, fecha:chararray);
agrupacion_usuarios = GROUP registros BY user_id;
peliculas_usuario = FOREACH agrupacion_usuarios GENERATE group, (COUNT(registros), AVG(registros.rating));

DUMP peliculas_usuario;
