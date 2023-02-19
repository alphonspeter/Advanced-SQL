with events_and_recipe as (
		select
    	event_id
        , session_id
        , event_timestamp
        , parse_json(event_details):recipe_id::string as recipe_id
        , parse_json(event_details):event::string as event_name
    from  
    	vk_data.events.website_activity
    group by 1,2,3,4,5
)

, session_time as (
			select 
            session_id 
			, max(event_timestamp) as maximum 
			, min(event_timestamp) as minimum
			, timediff(second, minimum, maximum) as total_time
from events_and_recipe
group by 1
)

, search_count as (
	select
    	session_id
        ,count(case when event_name = 'search' then event_id end) as search_count 
        , count_if(event_name = 'search') as cnt_search
    from 
    	events_and_recipe
    group by 
    	session_id
)

, top_recipe as (
	
    select 
    	event_timestamp::date as day
        , recipe_id           
        , count(*)    		  as highest_count
    from 
    	events_and_recipe
    where recipe_id is not null 
    group by 
    	1,2
    qualify 
    	row_number() over (partition by day order by highest_count desc) = 1
)

, base as 
(select 
    	event_timestamp::date 							   		   as date
        , count(s.session_id)   			   		   as session_id
        , ROUND(avg(t.total_time)) 	  				   as session_time_average
        , ROUND(avg(c.search_count))		  			   as search_amount
        , max(r.recipe_id)    		  					   as top_recipe_id
    from 
    	events_and_recipe s
    inner join session_time t
    on s.session_id = t.session_id
    inner join top_recipe r
    on t.minimum::date = r.day
    inner join search_count c
    on s.session_id = c.session_id
    group by date
    order by date)

select * from base ;

-- the query is structured in CTEs so we can check based on run where its causing issues in increasing data
-- the query ran in 641 ms
-- couldnt find the expensive node: as everything in query profile returned 0% : weird issue
