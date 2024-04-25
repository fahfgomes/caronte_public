SELECT
top(1)
A.FT01_NFE_CHAVE as "Chave", 
coalesce(A.FT01_NOTA,0) as "Numero", 
coalesce(A.FT01_DATA_EMB_H,'') as  "EmbarqueData", 
C.CD08_CGC_CLI as  "EmissorCNPJ",
C.CD08_NOME_CLI as  "EmissorRazao",
cast(A.FT01_DATA_EMISSAO as datetime) as  "EmissaoData",
SUM(B.FT02_VLR_MERC) as  "TotalMercadoria",
SUM(B.FT02_BASE_ICMS) as  "ICMSBaseCalculo",
SUM(B.FT02_ALIQ_ICMS) as  "ICMSAliquota",
SUM(B.FT02_VLR_ICMS) as  "ICMSValor",
0  "SeguroValor",
A.FT01_FRETE as  "FreteValor", 
A.FT01_QTDE_EMBAL as  "VolumesQuantidade",
A.FT01_PESO_LIQUIDO as  "PesoTotal", 
A.FT01_PESO_BRUTO as  "PesoDensidade",
CAST(A.FT01_OBS_ENTREGA  AS VARCHAR(MAX)) "Observacoes",
E.CD06_CGC_FOR  "TransportadoraCNPJ", 
E.CD06_NOME_FOR  "TransportadoraRazao",
SUM(B.FT02_VLR_TOTAL) as  "ValorTotalNF",
coalesce(A.FT01_NF_REMESSA_SERIE,0) as  "Serie", 
F.PV01_OPERADOR_LOGISTICO "CodCliente",
A.FT01_NFE_XML_ENVIADO  "NFeXML", 
C.CD08_ENDERECO as  "Logradouro", 
0 as  "Numero",
0 as  "Complemento",
C.CD08_BAIRRO as  "Bairro", 
BB.CD62_COD_IBGE as "CodigoMunicipio",
C.CD08_CIDADE as  "Municipio",
C.CD08_CEP as  "Cep", 
C.CD30_UF as  "UF",
C.CD08_TELEFONE as  "Fone",
'55' as  "CodigoPais",
'BRASIL' as  "Pais",
IIF(LEN(A.CD08_CGC_CLI) = 11,'XXXXXXXXXXX',A.CD08_CGC_CLI) as DestinatarioCNPJCPF,
E.CD06_FANTASIA_FOR as  "DestinatarioRazao", 
E.CD06_ENDERECO as  "Logradouro",
0 as  "Numero",
0 as  "Complemento",
E.CD06_BAIRRO as  "Bairro",
AA.CD62_COD_IBGE as  "CodigoMunicipio",
E.CD06_CIDADE as  "Municipio",
E.CD06_CEP as  "Cep",
E.CD30_UF as  "UF", 
E.CD06_TELEFONE as  "Fone", 
'55' as  "CodigoPais",
'BRASIL' as  "Pais",
E.CD06_EMAIL as  "Email",
B.CD04_MATERIAL as  "CodigoProduto", 
0 as  "Indice",
B.FT02_DESCRICAO as  "Descricao",
B.FT02_QTDE as  "Quantidade",
B.CD20_COF as  "CFOP",
coalesce(M1.CD04C_ATRIBUTO,0) as  "Altura",
coalesce(M2.CD04C_ATRIBUTO,0) as  "Largura",
coalesce(M3.CD04C_ATRIBUTO,0) as  "Profundidade",
SUM(D.CD04_CUBAGEM*B.FT02_QTDE) as  "PesoCubado",
A.FT01_PESO_LIQUIDO as  "PesoAferido" 
FROM T_FT_01 A WITH (NOLOCK)
JOIN T_FT_02 B WITH (NOLOCK) ON (B.SY01_EMPRESA = A.SY01_EMPRESA)
AND (B.FT01_NOTA = A.FT01_NOTA)
AND (B.CD14_SIGLA = A.CD14_SIGLA)
JOIN T_CD_08 C WITH (NOLOCK) ON (C.CD08_CGC_CLI = A.CD08_CGC_CLI)
JOIN T_CD_04 D WITH (NOLOCK) ON (D.CD04_MATERIAL = B.CD04_MATERIAL)
LEFT JOIN T_CD_06 E WITH (NOLOCK) ON (E.CD06_CGC_FOR = A.CD06_CGC_FOR)
LEFT JOIN T_PV_01 F WITH (NOLOCK) ON (F.SY01_EMPRESA = B.SY01_EMPRESA)
AND (F.PV01_PEDIDO = B.PV01_PEDIDO)
LEFT JOIN T_CD_05 G WITH(NOLOCK) ON (G.CD04_MATERIAL = D.CD04_MATERIAL)
LEFT OUTER JOIN dbo.T_CD_04C AS M1 WITH (NOLOCK) ON M1.CD04_MATERIAL = B.CD04_MATERIAL 
AND M1.CD23_CARACTERISTICA = 'ALTURA DO APARELHO (CM)' 
LEFT OUTER JOIN dbo.T_CD_04C AS M2 WITH (NOLOCK) ON M2.CD04_MATERIAL = B.CD04_MATERIAL 
AND M2.CD23_CARACTERISTICA = 'LARGURA DO APARELHO (CM)' 
LEFT OUTER JOIN dbo.T_CD_04C AS M3 WITH (NOLOCK) ON M3.CD04_MATERIAL = B.CD04_MATERIAL 
AND M3.CD23_CARACTERISTICA = 'PROFUNDIDADE DO APARELHO (CM)'
left join T_CD_62 BB WITH (NOLOCK) on (BB.CD62_MUNICIPIO = E.CD06_CIDADE)
AND (BB.CD30_UF = C.CD30_UF)
left join T_CD_62 AA WITH (NOLOCK) on (AA.CD62_MUNICIPIO = E.CD06_CIDADE)
AND (AA.CD30_UF = E.CD30_UF) 
WHERE
cast(A.FT01_DATA_EMISSAO as date) = CAST(GETDATE() AS DATE)  and A.FT01_NFE_CHAVE is not null
GROUP BY 
A.FT01_NFE_CHAVE, 
A.FT01_NOTA, 
A.FT01_DATA_EMB_H, 
C.CD08_CGC_CLI, 
C.CD08_NOME_CLI, 
A.FT01_DATA_EMISSAO, 
A.FT01_FRETE, 
A.FT01_QTDE_EMBAL,
A.FT01_PESO_LIQUIDO, 
A.FT01_PESO_BRUTO, 
CAST(A.FT01_OBS_ENTREGA  AS VARCHAR(MAX)), 
E.CD06_CGC_FOR,
E.CD06_NOME_FOR,
A.FT01_NF_REMESSA_SERIE,
F.PV01_OPERADOR_LOGISTICO,
A.FT01_NFE_XML_ENVIADO,
C.CD08_ENDERECO, 
C.CD08_BAIRRO,
BB.CD62_COD_IBGE,
C.CD08_CIDADE,
C.CD08_CEP,
C.CD30_UF,
C.CD08_TELEFONE, 
A.CD08_CGC_CLI,
E.CD06_FANTASIA_FOR,
E.CD06_ENDERECO, 
E.CD06_BAIRRO,
AA.CD62_COD_IBGE,
E.CD06_CIDADE,
E.CD06_CEP,
E.CD30_UF,
E.CD06_TELEFONE, 
E.CD06_EMAIL, 
B.CD04_MATERIAL, 
G.CD28_LINHA,
B.FT02_DESCRICAO, 
B.FT02_QTDE, 
B.CD20_COF,
M1.CD04C_ATRIBUTO,
M2.CD04C_ATRIBUTO,
M3.CD04C_ATRIBUTO,
A.FT01_PESO_LIQUIDO;
