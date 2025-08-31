with transacoes as (
    select 
        tr.pk_transacao
        , cast(tr.data_transacao as date) as data_transacao
        , tr.fk_conta
        , tr.valor_transacao
        , tr.nome_transacao
        , cb.valor_cambio
        , row_number() over (partition by tr.pk_transacao order by cb.data_cambio desc) as rn
    from {{ref('stg_transacoes')}} tr
    left join {{ ref('stg_ptax') }} cb on cb.data_cambio <= tr.data_transacao
)
, transacoes_dedup as (
    select *
    from transacoes
    qualify rn = 1
)
, clientes as (
    select distinct 
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
    select distinct
    ag.pk_agencia
    , ag.nome_da_agencia
    , ag.cidade
    , ag.estado
    , ag.tipo_de_agencia
    from {{ref('dim_agencias')}} ag
)
select
    tr.pk_transacao
    , tr.data_transacao
    , tr.valor_transacao
    , tr.valor_cambio
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

from transacoes_dedup tr
left join clientes cli on tr.fk_conta = cli.pk_conta
left join agencias ag on cli.fk_agencia = ag.pk_agencia