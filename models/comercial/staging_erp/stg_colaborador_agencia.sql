with COLABORADOR_AGENCIA as (
    select
    cast(cod_colaborador as int) as fk_colaborador
    , cast(cod_agencia as int) as fk_agencia

    from {{source('banvic','COLABORADOR_AGENCIA')}}
)
select * from COLABORADOR_AGENCIA