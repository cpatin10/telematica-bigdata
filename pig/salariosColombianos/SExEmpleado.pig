empleados = LOAD '/user/cpatin10/datasets/empleados.csv' USING PigStorage(',') AS (SE:int, Id_empleado:int, salario:int, year:int);
se_data = GROUP Id_empleado BY SE;
foreach_data = FOREACH se_data GENERATE COUNT(empleados.SE);
DUMP foreach_data;
