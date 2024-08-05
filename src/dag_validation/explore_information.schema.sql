SELECT 
            *
        FROM 
            BANANA_QUALITY.information_schema.tables LIMIT 10;
        --WHERE 
          --  table_schema = EXP 
           -- AND table_name = DAG_TRAIN;

SELECT *
FROM BANANA_QUALITY.INFORMATION_SCHEMA.;

select * from table(show tasks()) limit 1;
show tasks;

select * from table(snowflake.information_schema.task_history());


CREATE OR REPLACE VIEW banana_quality.exp.SNOWFLAKE_tasks AS
SHOW tasks;


-- Objetos afectados en el deployment:
show objects in 
    database banana_quality;
    --schema exp;

