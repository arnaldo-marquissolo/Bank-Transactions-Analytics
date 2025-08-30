with CLIENTES as (
    select
    cast(cod_cliente as int) as pk_cliente
    , cast(primeiro_nome as varchar) || ' ' || cast(ultimo_nome as varchar) as nome_completo_cliente
    , cast(tipo_cliente as varchar) as tipo_de_cliente
    , try_cast(split_part(data_inclusao, ' ', 1) as date) as data_inclusao_cliente
    , cast(data_nascimento as date) as data_de_nascimento_cliente
    , cast(endereco as varchar) as endereco_cliente
    , cast(CEP as varchar) as CEP


    from {{source('banvic', 'CLIENTES')}}
)
select * from CLIENTES