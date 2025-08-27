with COLABORADORES as (
    select
    cast(cod_colaborador as int) as pk_colaborador
    , cast(primeiro_nome as varchar) || ' ' || cast(ultimo_nome as varchar) as nome_completo_colaborador

    from {{source('banvic','COLABORADORES')}}
)
select * from COLABORADORES