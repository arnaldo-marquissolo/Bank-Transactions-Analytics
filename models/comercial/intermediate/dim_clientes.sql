with CLIENTES as (
    select
    cli.pk_cliente
    , cli.nome_completo_cliente
    , cli.tipo_de_cliente
    , cli.data_de_nascimento_cliente
    , con.pk_conta
    , con.fk_agencia
    , con.fk_colaborador
    , con.tipo_conta
    , con.saldo_total
    , con.saldo_disponivel
    , con.data_ultimo_lancamento
    , tr.pk_transacao
    , tr.data_transacao
    , tr.nome_transacao
    , tr.valor_transacao

    from {{ref("stg_clientes")}} cli
    left join {{ref('stg_contas')}} con on cli.pk_cliente = con.fk_cliente
    left join {{ref('stg_transacoes')}} tr on con.pk_conta = tr.fk_conta
)
select * from CLIENTES