registros = LOAD '/user/cpatin10/datasets/peliculas.csv' USING PigStorage(',') AS (user_id:int, movie_id:int, rating:float, genero:chararray, fecha:chararray);
agrupacion_dias = GROUP registros BY fecha;
promedio_calificacion = FOREACH agrupacion_dias GENERATE group, AVG(registros.rating) as promedio;
agrupacion_promedio = GROUP promedio_calificacion ALL;
dia_menor = FOREACH agrupacion_promedio GENERATE MIN(promedio_calificacion.promedio) as menor_promedio;

DUMP agrupacion_promedio.$1;

/*
filtro_menor = FILTER agrupacion_promedio BY promedio_calificacion.promedio == dia_menor.menor_promedio;

DUMP filtro_menor;

*/