with transacoes as (
    select
        tr.pk_transacao
        , cast(tr.data_transacao as date) as data_transacao
        , tr.fk_conta
        , tr.valor_transacao
        , tr.nome_transacao
    from {{ref('stg_transacoes')}} tr
)
, clientes as (
    select
    cli.pk_cliente
    , cli.nome_completo_cliente
    , cli.data_de_nascimento_cliente
    , con.pk_conta
    , cli.tipo_conta
    , con.fk_agencia
    from {{ref('dim_clientes')}} cli
    left join {{ref('stg_contas')}} con on cli.pk_cliente = con.fk_cliente
)
, agencias as (
    select 
    ag.pk_agencia
    , ag.nome_da_agencia
    , ag.cidade
    , ag.estado
    , ag.tipo_de_agencia
    from {{ref('dim_agencias')}} ag
)
, cambio as (
    select
    cb.data_cambio
    , cb.valor_cambio
    from {{ref('stg_ptax')}} cb
)
select
    tr.pk_transacao
    , tr.data_transacao
    , tr.valor_transacao
    , cb.valor_cambio
    , tr.nome_transacao
    , cli.tipo_conta
    , cli.pk_cliente as fk_cliente
    , cli.data_de_nascimento_cliente
    , cli.nome_completo_cliente
    , cli.fk_agencia as fk_agencia
    , ag.nome_da_agencia
    , ag.cidade
    , ag.estado
    , ag.tipo_de_agencia

from transacoes tr
left join clientes cli on tr.fk_conta = cli.pk_conta
left join agencias ag on cli.fk_agencia = ag.pk_agencia
left join cambio cb on tr.data_transacao = cb.data_cambio