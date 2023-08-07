select id, name, case
    when amount >= 6.0 then 'Y'
    else 'N'
end as flag from {table}
