with CONTAS as (
    select
    cast(num_conta as int) pk_conta
    , cast(cod_cliente as int) as fk_cliente
    , cast(cod_agencia as int) as fk_agencia
    , cast(cod_colaborador as int) as fk_colaborador
    , cast(tipo_conta as varchar) as tipo_conta
    , try_cast(split_part(data_abertura, ' ', 1) as date) as data_abertura_conta
    , cast(saldo_total as numeric(18,2)) as saldo_total
    , cast(saldo_disponivel as numeric(18,2)) as saldo_disponivel
    , try_cast(split_part(data_ultimo_lancamento, ' ', 1) as date) as data_ultimo_lancamento

    from {{source('banvic','CONTAS')}}
)
select * from CONTAS