with TRANSACOES as (
    select
    cast(cod_transacao as int) as pk_transacao
    , cast(num_conta as int) as fk_conta
    , try_cast(split_part(data_transacao, ' ', 1) as date) as data_transacao
    , cast(nome_transacao as varchar) as nome_transacao
    , cast(valor_transacao as numeric(18,2)) as valor_transacao

    from {{source('banvic','TRANSACOES')}}
)
select * from TRANSACOES