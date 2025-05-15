/*
    Consulta 7: Relação entre avaliação de compra e categoria de produto
    Objetivo: Verificar se certas categorias de produtos recebem avaliações consistentemente melhores ou piores,
*/
--           o que pode indicar qualidade do produto, precisão da descrição, etc.
SELECT
    dp.categoria,
    fv.purchase_rating AS avaliacao_da_compra,
    COUNT(fv.sk_venda) AS total_transacoes_com_esta_avaliacao
FROM
    FATO_VENDAS fv
JOIN
    DIM_PRODUTO dp ON fv.sk_produto = dp.sk_produto
WHERE
    fv.purchase_rating IS NOT NULL -- Considera apenas vendas com avaliação
    AND dp.sk_produto != -1        -- Exclui produtos 'Desconhecidos'
GROUP BY
    dp.categoria,
    fv.purchase_rating
ORDER BY
    dp.categoria,
    fv.purchase_rating;