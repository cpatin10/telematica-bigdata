empleados = LOAD '/user/cpatin10/datasets/empleados.csv' USING PigStorage(',') AS (SE:int, Id_empleado:int, salario:int, year:int);
se_data = GROUP empleados BY SE;
foreach_data = FOREACH se_data GENERATE group, AVG(empleados.salario);
DUMP foreach_data;
STORE foreach_data INTO '/user/cpatin10/pig/outPromedioSe' USING PigStorage (',');