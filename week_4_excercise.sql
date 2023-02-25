-- Filtering out urgent automobile orders with needed fields
WITH AUTOMBILE_URGENT_ORDERS AS (
        SELECT 
        	C_CUSTKEY
            , O_ORDERKEY
            , O_ORDERDATE
            , L_PARTKEY
            , L_QUANTITY
            ,L_EXTENDEDPRICE
            ,RANK() OVER (PARTITION BY C_CUSTKEY ORDER BY L.L_EXTENDEDPRICE DESC) AS RANK  
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS O
        LEFT JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER C ON C.C_CUSTKEY =O.O_CUSTKEY
        LEFT JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM L ON O.O_ORDERKEY = L.L_ORDERKEY
        	WHERE O_ORDERPRIORITY = '1-URGENT' 	
            	AND C_MKTSEGMENT = 'AUTOMOBILE'
        ORDER BY 1,2 ASC
)
 
-- ranking and picking top 3 highest spent orders and listing the orders together
, VALUABLE_ORDERS AS (
            SELECT 
            	C_CUSTKEY
                , MAX(O_ORDERDATE) 			AS LAST_ORDER_DATE
                , LISTAGG(O_ORDERKEY, ',') 		AS ORDER_NUMBERS
                , SUM(L_EXTENDEDPRICE)			AS TOTAL_SPENT
            FROM AUTOMBILE_URGENT_ORDERS
            	WHERE RANK <= 3
            GROUP BY 1
            ORDER BY 1
)

-- main query with part, quantity and spent based on the ranking of orders
SELECT 
        V.C_CUSTKEY
        , V.LAST_ORDER_DATE
        , V.ORDER_NUMBERS
        , V.TOTAL_SPENT
        , MAX(CASE WHEN RANK =1 THEN A.L_PARTKEY END) 			AS PART_1_KEY 
        , MAX(CASE WHEN RANK =1 THEN A.L_QUANTITY END) 			AS PART_1_QUANTITY
    	, MAX(CASE WHEN RANK =1 THEN A.L_EXTENDEDPRICE END) 	        AS PART_1_TOTAL_SPENT 
    	, MAX(CASE WHEN RANK =2 THEN A.L_PARTKEY END) 			AS PART_2_KEY 
        , MAX(CASE WHEN RANK =2 THEN A.L_QUANTITY END) 			AS PART_2_QUANTITY
    	, MAX(CASE WHEN RANK =2 THEN A.L_EXTENDEDPRICE END) 		AS PART_2_TOTAL_SPENT 
    	, MAX(CASE WHEN RANK =3 THEN A.L_PARTKEY END) 			AS PART_3_KEY 
        , MAX(CASE WHEN RANK =3 THEN A.L_QUANTITY END) 			AS PART_3_QUANTITY
    	, MAX(CASE WHEN RANK =3 THEN A.L_EXTENDEDPRICE END)		AS PART_3_TOTAL_SPENT 
FROM VALUABLE_ORDERS V
LEFT JOIN AUTOMBILE_URGENT_ORDERS A ON V.C_CUSTKEY = A.C_CUSTKEY
WHERE RANK <= 3
GROUP BY 1,2,3,4
ORDER BY 2 DESC
LIMIT 100;


/* Regarding evanluation of the candidate's Query
1. No particular support of CTE's with comments
2. Couldnt find the need to use the TABLE : 'PART' as TABLE ='ORDERS' is suffice for the business question
2. The multiple inner joins could prove confusing ans costly 
4. Results producing duplicates 
*/






