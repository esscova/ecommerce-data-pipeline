/*
    Consulta 9: Vendas nos Finais de Semana vs. Dias Úteis
    Objetivo: Comparar o volume de vendas e o valor entre dias úteis e fins de semana.
    'eh_fimdesemana' é calculado dinamicamente a partir de data_completa.
*/

SELECT
    CASE
        WHEN EXTRACT(DOW FROM dt.data_completa) IN (0, 6) THEN 'Fim de Semana'  0 = Domingo, 6 = Sábado
        ELSE 'Dia Útil'
    END AS tipo_dia,
    COUNT(fv.sk_venda) AS total_transacoes_venda,
    SUM((fv.preco_cents + COALESCE(fv.shipping_cost_cents, 0)) / 100.0) AS valor_total_vendas,
    AVG((fv.preco_cents + COALESCE(fv.shipping_cost_cents, 0)) / 100.0) AS ticket_medio_por_transacao
FROM
    FATO_VENDAS fv
JOIN
    DIM_TEMPO dt ON fv.sk_tempo = dt.sk_tempo
GROUP BY
    tipo_dia    Agrupa pelo resultado da expressão CASE
ORDER BY
    tipo_dia;