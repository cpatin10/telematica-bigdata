registros = LOAD '/user/cpatin10/datasets/peliculas.csv' USING PigStorage(',') AS (user_id:int, movie_id:int, genero:chararray, rating:float, fecha:chararray);
agrupacion_usuarios = GROUP registros BY user_id;
peliculas_usuario = FOREACH agrupacion_usuarios GENERATE group, (COUNT(registros), AVG(registros.rating));

DUMP peliculas_usuario;
