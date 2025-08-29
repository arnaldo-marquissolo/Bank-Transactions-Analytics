 with AGENCIAS as (
    select
    a.pk_agencia
    , a.nome_da_agencia
    , a.endereco
    , a.cidade
    , a.estado
    , a.tipo_de_agencia
    , ca.fk_colaborador
    , col.nome_completo_colaborador
    
    from {{ref('agencias')}} a
    left join {{ref('colaborador_agencia')}} ca on a.pk_agencia = ca.fk_agencia
    left join {{ref('colaboradores')}} col on ca.fk_colaborador = col.pk_colaborador

)
select * from AGENCIAS