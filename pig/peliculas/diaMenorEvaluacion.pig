registros = LOAD '/user/cpatin10/datasets/peliculas.csv' USING PigStorage(',') AS (user_id:int, movie_id:int, rating:float, genero:chararray, fecha:chararray);
agrupacion_dias = GROUP registros BY fecha;
promedio_calificacion = FOREACH agrupacion_dias GENERATE group, AVG(registros.rating) as promedio;
agrupacion_promedio = GROUP promedio_calificacion ALL;
dia_menor = FOREACH agrupacion_promedio GENERATE promedio_calificacion, MIN(promedio_calificacion.promedio) as menor_promedio;

DUMP promedio_calificacion.promedio;

/*
filtro_menor = FILTER agrupacion_promedio BY promedio_calificacion.promedio == dia_menor.menor_promedio;

DUMP filtro_menor;

*/