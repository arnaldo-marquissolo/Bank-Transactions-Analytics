with TRANSACOES as (
    select
    tr.pk_transacao
    , tr.fk_conta
    , tr.data_transacao
    , tr.nome_transacao
    , tr.valor_transacao
    , ptax.valor_cambio as valor_cambio
    
    
    from {{ref('stg_transacoes')}} tr
    left join {{ref("stg_ptax")}} ptax on ptax.data_cambio = (select max(data_cambio) from {{ref("stg_ptax")}} where data_cambio <= tr.data_transacao)
)
select * from TRANSACOES