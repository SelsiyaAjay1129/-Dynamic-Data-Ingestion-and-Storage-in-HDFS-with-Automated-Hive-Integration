use population_data;

select count(*) from population_status;

select * from population_status limit 100;


-- Check for Null Values in Critical Columns
SELECT * FROM population_status
WHERE 
    CENSUS2010POP IS NULL OR
    ESTIMATESBASE2010 IS NULL OR
    POPESTIMATE2010 IS NULL OR
    POPESTIMATE2011 IS NULL OR
    POPESTIMATE2012 IS NULL OR
    POPESTIMATE2013 IS NULL OR
    POPESTIMATE2014 IS NULL;


-- Check for Duplicate Rows
SELECT NAME, STATE, COUNTY, COUNT(*)
FROM population_status
GROUP BY NAME, STATE, COUNTY
HAVING COUNT(*) > 1;


-- Check for Invalid Data in Specific Columns
SELECT * FROM population_status
WHERE FUNCSTAT NOT IN ('A', 'B');


-- Check for Population Decrease Over Time
SELECT * FROM population_status
WHERE 
    POPESTIMATE2011 < POPESTIMATE2010 OR
    POPESTIMATE2012 < POPESTIMATE2011 OR
    POPESTIMATE2013 < POPESTIMATE2012 OR
    POPESTIMATE2014 < POPESTIMATE2013;
