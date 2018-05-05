empleados = LOAD '/user/cpatin10/datasets/empleados.csv' USING PigStorage(',') AS (SE:int, Id_empleado:int, salario:int, year:int);
se_data = GROUP empleados BY Id_empleado;
foreach_data = FOREACH se_data GENERATE empleados.Id_empleado, AVG(empleados.salario);
DUMP foreach_data;
STORE foreach_data INTO '/user/username/pig/outPromedioEmpleados' USING PigStorage (',');