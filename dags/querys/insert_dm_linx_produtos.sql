insert into dm_linx.linx_produtos select *, (current_date -1)::date as dt_carga from db_raw.linx_produtos_raw