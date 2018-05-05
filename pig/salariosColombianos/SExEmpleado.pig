empleados = LOAD '/user/cpatin10/datasets/empleados.csv' USING PigStorage(',') AS (SE:int, Id_empleado:int, salario:int, year:int);
se_data = GROUP empleados BY Id_empleado;

se_count = FOREACH se_data {
        se_distinct = DISTINCT empleados.SE;
        GENERATE group, COUNT(se_distinct);        
    };
DUMP se_count;

