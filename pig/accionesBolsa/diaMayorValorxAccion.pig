acciones = LOAD '/user/cpatin10/datasets/empresas.csv' USING PigStorage(',') AS (empresa:chararray, valor:float, fecha:chararray);
agrupacion_empresa = GROUP acciones BY empresa;


/*
dia_menor = FOREACH agrupacion_empresa GENERATE group, MIN(acciones.valor);
*/


dia_menor = FOREACH agrupacion_empresa {
    menor_valor = MIN(acciones.valor);

    filtro_menor_valor = FILTER acciones BY acciones.valor == menor_valor;
    /*dump filtro_menor_valor;*/

    GENERATE filtro_menor_valor;
};


/*
dia_menor = FOREACH (FILTER agrupacion_empresa BY acciones.valor <= 22.0f) GENERATE group, acciones.valor;

*/


DUMP dia_menor;


/*
foreach_data = FOREACH se_data GENERATE group, AVG(empleados.salario);
DUMP foreach_data; */
/* STORE foreach_data INTO '/user/cpatin10/pig/outDiaMayorValorxAccion' USING PigStorage (','); */


