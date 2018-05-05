registros = LOAD '/user/cpatin10/datasets/peliculas.csv' USING PigStorage(',') AS (user_id:int, movie_id:int, rating:float, genero:chararray, fecha:chararray);
agrupacion_dias = GROUP registros BY fecha;
promedio_calificacion = FOREACH agrupacion_dias GENERATE group, AVG(registros.rating) as promedio;
agrupacion_promedio = GROUP promedio_calificacion ALL;
dia_menor = FOREACH agrupacion_promedio GENERATE promedio_calificacion, MIN(promedio_calificacion.promedio);/*, MIN(promedio_calificacion.promedio); */

DUMP dia_menor;

/*
agrupacion_peliculas = GROUP registros BY fecha;
conteo_peliculas = FOREACH agrupacion_peliculas GENERATE group, COUNT(registros) as conteo;
agrupacion_conteo = GROUP conteo_peliculas ALL;

dia_menor = FOREACH agrupacion_conteo GENERATE conteo_peliculas, MIN(conteo_peliculas.conteo);

DUMP dia_menor;

*/