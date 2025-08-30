with AGENCIAS as (
    select
    cast(cod_agencia as int) as pk_agencia
    , cast(nome as varchar) as nome_da_agencia
    , cast(endereco as varchar) as endereco
    , cast(cidade as varchar) as cidade
    , cast(uf as varchar) as estado
    , cast(data_abertura as date) as data_de_abertura
    , cast(tipo_agencia as varchar) as tipo_de_agencia  

    from {{ source('banvic','AGENCIAS')}}
)
select * from AGENCIAS