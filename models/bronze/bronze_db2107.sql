with joined as (
    {{ union_versions('v2107', ['00','01','02','03','04']) }}
)

select * from joined
