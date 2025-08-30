with PTAX as (
    select
    cast(replace(ptx, ',', '.') as numeric(18,4)) as valor_cambio
    , cast(datahoracotacao as date) as data_cambio

    from {{source('banvic','PTAX')}}
)
select * from PTAX order by data_cambio 