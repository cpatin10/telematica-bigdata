acciones = LOAD '/user/cpatin10/datasets/empresas.csv' USING PigStorage(',') AS (empresa:chararray, valor:float, fecha:chararray);
agrupacion_empresa = GROUP acciones BY empresa;




dia_menor = FOREACH agrupacion_empresa {
    menor_valor = 22.0F; 


    filtro_menor_valor = FILTER acciones BY valor <= menor_valor;

    GENERATE group, filtro_menor_valor.fecha;
};


DUMP dia_menor;


/*
foreach_data = FOREACH se_data GENERATE group, AVG(empleados.salario);
DUMP foreach_data; */
/* STORE foreach_data INTO '/user/cpatin10/pig/outDiaMayorValorxAccion' USING PigStorage (','); */


