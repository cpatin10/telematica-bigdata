registros = LOAD '/user/cpatin10/datasets/peliculas.csv' USING PigStorage(',') AS (user_id:int, movie_id:int, rating:float, genero:chararray, fecha:chararray);
agrupacion_genero = GROUP registros BY genero;
promedio_genero = FOREACH agrupacion_genero GENERATE group, AVG(registros.rating) as promedio;
agrupacion_promedio = GROUP promedio_genero ALL;
dia_menor = FOREACH agrupacion_promedio GENERATE promedio_genero, MIN(promedio_genero.promedio);

DUMP dia_menor;
STORE dia_menor INTO '/user/cpatin10/pig/outDiaMenosPeliculasVistas' USING PigStorage (',');