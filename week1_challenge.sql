/* steps

1. Use Cities CTE and Customer Location CTE to get eligible customers
2. If the customer filled up survey, then give a rank to their preferences and also use customer id from above CTE to get customers that can be reached
3. Pivot and take upto three preferences
4. Flatten the recipe table, and connect preferneces to the flatten output based on rank =1 ; SInce the flattened table might have a lot of options in it only connect with  'preference 1'
5. finally connect back to customer_id to get customer details and preferneces along with recipe

*/


with cities as (
          select distinct city_name as city_name
        , state_abbr as state_abbr
        , lat
        , long
    from resources.us_cities
    qualify row_number() over(partition by city_name, state_abbr order by city_name) = 1
    order by 2,1),

customer_location as(
select c.customer_id, c.first_name,c.last_name, c.email, a.customer_city, a.customer_state
        ,r.Lat, r.Long
from customers.customer_data c
left join customers.customer_address a on c.customer_id =a.customer_id
inner join cities r on TRIM(UPPER(a.customer_city)) = TRIM(r.city_name) and TRIM(a.customer_state) = TRIM(r.state_abbr)
),

preference as (
    select s.customer_id, tag_property,
            row_number() over (partition by customer_id order by customer_id) as rank_preference
            from customers.customer_survey s
            left join resources.recipe_tags r on s.tag_id = r.tag_id
              where s.is_active = true and customer_id in (select distinct customer_id from customer_location) 
              order by customer_id) ,
              
rank_3 as (
select * from preference
where rank_preference <= 3), 

pivot_table as(
select * from rank_3 
pivot( MIN(tag_property) for rank_preference in ( '1' , '2' , '3')) 
as p (customer_id, Preference_1 , Preference_2 , Preference_3)
order by 1 ), 

final_preference as (select p. customer_id,
       l.first_name,
       l.last_name,
       l.email,
       trim(p.preference_1) as preference_1,
       trim(p.preference_2) as preference_2,
       trim(p.preference_3) as preference_3
from pivot_table p
left join customer_location l on p.customer_id = l.customer_id
order by l.email asc ),        
    
flatten_recipe as 
(select 
    recipe_id,
    recipe_name,
    trim(replace(flat_tag_list.value, '"', '')) as flat_tags
from vk_data.chefs.recipe
, table(flatten(tag_list)) as flat_tag_list ) ,

suggested_recipe as(
    select
        final_preference.customer_id,
        min(recipe_name) as recipe_name
    from final_preference
    inner join flatten_recipe on flat_tags = preference_1
    group by 1
),

result as (
    select 
        final_preference.customer_id,
        final_preference.email,
        final_preference.first_name,
        preference_1,
        preference_2,
        preference_3,
        recipe_name
    from  final_preference
    inner join suggested_recipe on final_preference.customer_id = suggested_recipe.customer_id
    order by 2
)

select *
from result;
