empleados = LOAD '/user/cpatin10/datasets/empleados.csv' USING PigStorage(',') AS (SE:int, Id_empleado:int, salario:int, year:int);
se_data = GROUP empleados BY Id_empleado;
foreach_data = FOREACH se_data GENERATE Id_empleado, COUNT(empleados.SE);
DUMP foreach_data;
