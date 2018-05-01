empleados = LOAD '/user/cpatin10/datasets/empleados.csv' USING PigStorage(',') AS (SE:int, Id_empleado:int, salario:int, year:int);
se_data = GROUP empleados BY Id_empleado;
distinct_se = DISTINCT empleados.SE;
foreach_data = FOREACH empleados GENERATE empleados.Id_empleado, COUNT(distinct_se.SE);
DUMP foreach_data;
