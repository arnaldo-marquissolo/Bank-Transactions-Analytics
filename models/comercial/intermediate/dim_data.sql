with TRANSACOES as (
    select
    tr.pk_transacao
    , tr.fk_conta
    , tr.data_transacao
    , tr.nome_transacao
    , tr.valor_transacao
    , ptax.valor_cambio as valor_cambio
    , row_number() over (partition by tr.pk_transacao order by ptax.data_cambio desc) as rn
    
    from {{ref('stg_transacoes')}} tr
    left join {{ref("stg_ptax")}} ptax on ptax.data_cambio = (select max(data_cambio) from {{ref("stg_ptax")}} where data_cambio <= tr.data_transacao)
)
select
    pk_transacao
    , fk_conta
    , data_transacao
    , nome_transacao
    , valor_transacao
    , valor_cambio
from transacoes qualify rn = 1