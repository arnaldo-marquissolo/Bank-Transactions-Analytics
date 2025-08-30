with PROPOSTAS_CREDITO as (
    select
    cast(cod_proposta as int) as pk_proposta
    , cast(cod_cliente as int) as fk_cliente
    , cast(cod_colaborador as int) as fk_colaborador
    , try_cast(split_part(data_entrada_proposta, ' ', 1) as date) as data_entrada_proposta
    , cast(taxa_juros_mensal as numeric(18,2)) as taxa_de_juros_mensal
    , cast(valor_proposta as numeric(18,2)) as valor_da_proposta
    , cast(valor_financiamento as numeric(18,2)) as valor_do_financiamento
    , cast(valor_entrada as numeric(18,2)) as valor_da_entrada
    , cast(valor_prestacao as numeric(18,2)) as valor_da_prestacao
    , cast(quantidade_parcelas as int) as quantidade_parcelas
    , cast(carencia as int) as qtd_carencia
    , cast(status_proposta as varchar) as status_proposta

    from {{source('banvic','PROPOSTAS_CREDITO')}}
)
select * from PROPOSTAS_CREDITO