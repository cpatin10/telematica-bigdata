registros = LOAD '/user/cpatin10/datasets/peliculas.csv' USING PigStorage(',') AS (user_id:int, movie_id:int, genero:chararray, rating:float, fecha:chararray);
agrupacion_peliculas = GROUP registros BY fecha;
conteo_peliculas = FOREACH agrupacion_peliculas GENERATE group, COUNT(registros) as conteo;
agrupacion_conteo = GROUP conteo_peliculas ALL;

dia_menor = FOREACH agrupacion_conteo GENERATE conteo_peliculas.group, MIN(conteo_peliculas.conteo);

DUMP dia_menor;
