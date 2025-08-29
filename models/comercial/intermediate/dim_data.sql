with TRANSACOES as (
    select
    tr.pk_transacao
    , tr.fk_conta
    , tr.data_transacao
    , tr.nome_transacao
    , tr.valor_transacao
    , ptax.valor_cambio
    
    
    from {{ref('transacoes')}} tr
    left join {{ref("ptax")}} ptax on ptax.data_cambio = (select max(data_cambio) from {{ref("ptax")}} where data_cambio <= tr.data_transacao)
)
select * from TRANSACOES order by data_transacao