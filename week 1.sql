with cities as (
          select distinct city_name as city_name
        , state_abbr as state_abbr
        , lat
        , long
    from resorces.us_cities
    qualify row_number() over(partition by city_name, state_abbr order by city_name) = 1
    order by 2,1),

customer_location as
(select c.customer_id, c.first_name,c.last_name, a.customer_city, a.customer_state
     ,r.Lat, r.Long
from customers.customer_data c
left join customers.customer_address a on c.customer_id =a.customer_id
inner join cities r on TRIM(UPPER(a.customer_city)) = TRIM(r.city_name) and TRIM(a.customer_state) = TRIM(r.state_abbr)
) , 
                             
supplier_location as (select s.*
        , r.Lat, r.Long 
from suppliers.supplier_info s
left join cities r on TRIM(r.city_name) = TRIM(UPPER(s.supplier_city)) 
                                and TRIM(r.state_abbr) = TRIM(s.supplier_state) 
),

distance as ( 
select c.*, s.*,  
haversine(c.lat, c.long, s.lat, s.long) as distance_from_supplier,
st_distance(st_makepoint(c.long, c.lat), st_makepoint( s.long, s.lat)) / 1000 as new_distance, 
distance_from_supplier * 0.621371 as distance_in_miles,
rank() over (partition by customer_id order by haversine(c.lat, c.long, s.lat, s.long) asc) as distance_rank
from customer_location c 
cross join supplier_location s   
)

select d.customer_id, d.first_name, d.last_name, d.customer_city, d.customer_state, d.supplier_name, d.supplier_city, d.supplier_state, d.distance_from_supplier, d.distance_in_miles, d.distance_rank
from distance d
where distance_rank = 1
order by 3,2 asc ;
