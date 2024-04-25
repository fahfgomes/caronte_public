insert into dm_bi.linx_produtos
(codigo_produto
,fornecedor
,nome_fornecedor
,descricao_produto
,categoria
,familia
,marca
,origem
,referencia
,codauxiliar
,preco_venda
,codebar
,dt_carga)
select 
codigo_produto
,fornecedor 
,nome_fornecedor 
,descricao_produto 
,categoria 
,familia 
,marca 
,origem 
,referencia 
,codauxiliar 
,replace(preco_venda, ',', '.')::decimal(10,2) as preco_venda
,codebar 
,dt_carga
from dm_linx.linx_produtos
where dt_carga = current_date-1;