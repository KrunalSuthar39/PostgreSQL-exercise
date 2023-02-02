-------------------------------Querying Data-------------------------------------------
select * from payment;
select distinct customer_id from payment;
select p.payment_date as date_new from payment p;
select * from payment order by staff_id;

-------------------------------Filtering Data-------------------------------------------
select f.film_id, f.title 
from film f
where release_year between 2006 and 2018 OR rental_duration In(3,4,5,6,7,8) OR description like 'A%'
order by film_id 
offset 5 rows
fetch first 10 rows only;

select * from film
where release_year is not null
limit 50;

-------------------------------Set Operations------------------------------------------

 select * from top_rated_films
 union
 select * from most_popular_films
 
 select * from top_rated_films
 intersect
 select * from most_popular_films
 
 
 select * from top_rated_films
 except
 select * from most_popular_films
 
-------------------------------Subquery-------------------------------------------

SELECT
    film_id,
    title,
    length
FROM
    film
WHERE
    length > ANY (
            SELECT
                AVG (length)
            FROM
                film
            GROUP BY
                rating
    )
ORDER BY
    length;
------------------------------------------------------------------------	
SELECT
    film_id,
    title,
    length
FROM
    film
WHERE
    length > ALL (
            SELECT
                AVG (length)
            FROM
                film
            GROUP BY
                rating
    )
ORDER BY
    length;
---------------------------------------------------------------------------------------

SELECT
	first_name,
	last_name
FROM
	customer
WHERE
	EXISTS (
		SELECT 1
		FROM payment
		WHERE payment.customer_id = customer.customer_id
	);
	
-------------------------------Data Types-------------------------------------------
CREATE EXTENSION "uuid-ossp";
CREATE EXTENSION hstore;

CREATE TABLE krunaldatademo (
	ident_name INT GENERATED ALWAYS AS IDENTITY,
	pkey UUID NOT NULL DEFAULT uuid_generate_v1() , 
    ser_name BIGSERIAL,
    cust_ser_name INTEGER DEFAULT nextval('table_name_id_seq'),
    int_name INT,
    num_name NUMERIC(5,2),
    small_int_name SMALLINT,
    boolean_name BOOLEAN,
    char_name CHAR (1),
    varchar_name VARCHAR (10),
    text_name TEXT,
    date_name DATE,
    time_name TIME,
    ts_name TIMESTAMP, 
    tstz_name TIMESTAMPTZ,
    json_name json NOT NULL,
    hstore_name hstore,
    arr_name TEXT [],
    cust_name CUSTOM_DATATYPE    
);
-------------------------------Constrains-------------------------------------------
CREATE TABLE demo2(
   customer_id INT GENERATED ALWAYS AS IDENTITY,
   customer_name VARCHAR(255) NOT NULL,
   PRIMARY KEY(customer_id)
);

Create table krunalConstraintsDemo(
	po_no INTEGER PRIMARY KEY NOT NULL,
	birth_date DATE CHECK (birth_date > '1900-01-01'),
	email VARCHAR (50) UNIQUE,
	constraint fk_krunal
		foreign key(po_no)
			references demo2(customer_id)
);

-------------------------------CTE-------------------------------------------

WITH film_length AS
(
	SELECT 
	film_id,
	title,
	(CASE 
	 	WHEN length < 30 THEN 'Short'
	 	WHEN length < 90 THEN 'Medium'
		ELSE 'Long'
	 END ) AS length
	
FROM film )

SELECT
	film_id,
	title,
	length
FROM film_length
WHERE length = 'Short'
ORDER BY film_id;
-------------------------------------------------------------------
WITH RECURSIVE emp_hierarchy AS
(
	SELECT 
		employee_id,
		manager_id,
		full_name
	FROM employees
	WHERE employee_id = 4

	UNION
	
	SELECT 
		e.employee_id,
		e.manager_id,
		e.full_name
	FROM employees e
	JOIN emp_hierarchy em
	ON em.employee_id = e.manager_id
)

SELECT *
FROM emp_hierarchy;

-------------------------------Modifying Data-------------------------------------------
CREATE TABLE krunalEMP (
	id SERIAL PRIMARY KEY,
	name VARCHAR (50) NOT NULL,
	occupation VARCHAR (50)
);

INSERT INTO 
	krunalEMP (name, occupation)
VALUES
	('Krunal', 'Data Engineer');
	
INSERT INTO 
	krunalEMP (name, occupation)
VALUES
	('Isha', 'Business Analyst'),
	('Hardik', 'Web Research Executive'),
	('Tarun', 'Python Developer'),
	('Shubham', 'Python developer');
	select * from krunalEMP;

UPDATE krunalEMP
SET occupation = 'Data Engineer'
WHERE name = 'Shubham';


----update join---------------------------------------------
CREATE TABLE product_segment (
    id SERIAL PRIMARY KEY,
    segment VARCHAR NOT NULL,
    discount NUMERIC (4, 2)
);


INSERT INTO 
    product_segment (segment, discount)
VALUES
    ('Grand Luxury', 0.05),
    ('Luxury', 0.06),
    ('Mass', 0.1);
CREATE TABLE product(
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    price NUMERIC(10,2),
    net_price NUMERIC(10,2),
    segment_id INT NOT NULL,
    FOREIGN KEY(segment_id) REFERENCES product_segment(id)
);


INSERT INTO 
    product (name, price, segment_id) 
VALUES 
    ('diam', 804.89, 1),
    ('vestibulum aliquet', 228.55, 3),
    ('lacinia erat', 366.45, 2),
    ('scelerisque quam turpis', 145.33, 3),
    ('justo lacinia', 551.77, 2),
    ('ultrices mattis odio', 261.58, 3),
    ('hendrerit', 519.62, 2),
    ('in hac habitasse', 843.31, 1),
    ('orci eget orci', 254.18, 3),
    ('pellentesque', 427.78, 2),
    ('sit amet nunc', 936.29, 1),
    ('sed vestibulum', 910.34, 1),
    ('turpis eget', 208.33, 3),
    ('cursus vestibulum', 985.45, 1),
    ('orci nullam', 841.26, 1),
    ('est quam pharetra', 896.38, 1),
    ('posuere', 575.74, 2),
    ('ligula', 530.64, 2),
    ('convallis', 892.43, 1),
    ('nulla elit ac', 161.71, 3);
	
UPDATE 
    product p
SET 
    net_price = price - price * discount
FROM 
    product_segment s
WHERE 
    p.segment_id = s.id;
	
SELECT * FROM product;
------------------------------------------------------------------------------
DELETE FROM krunalEMP
WHERE name ='Shubham';


CREATE TABLE krunal_cust (
	customer_id serial PRIMARY KEY,
	name VARCHAR UNIQUE,
	email VARCHAR NOT NULL
);

INSERT INTO 
    krunal_cust (name, email)
VALUES 
    ('Krunal', 'krunal345@ibm.com'),
    ('Arjun', 'arjun123@microsoft.com'),
    ('Shubham', 'shubham555@intel.com');
	
INSERT INTO 
    krunal_cust (name, email)
VALUES 
    ('Krunal', 'krunal345@ibm.com')
ON CONFLICT (name)
DO NOTHING;
-------------------------------Transactions-------------------------------------------
BEGIN;
	
	UPDATE actor_count
	SET count = count - 1;

	
	UPDATE actor_count
	SET count = count + 1;
	
	
	SELECT * FROM actor_count;
	
	ROLLBACK;
---------------------------------------------------------------------	
	
BEGIN;
	
	UPDATE actor_count
	SET count = count - 1;

	
	UPDATE actor_count
	SET count = count + 1;
	
	
	SELECT * FROM actor_count;
	
	COMMIT;

-------------------------------Managing Tables-------------------------------------------
CREATE TABLE krunalNew
(
	user_id serial PRIMARY KEY,
	username VARCHAR(50) NOT NULL,
	password VARCHAR(50),
	email VARCHAR(50) NOT NULL UNIQUE,
	created_on TIMESTAMP
);

SELECT c.*
INTO TABLE customer_japan
FROM customer c
JOIN address a USING (address_id)
JOIN city ci USING (city_id)
JOIN country co USING (country_id)
WHERE co.country = 'Japan'
ORDER BY c.customer_i

CREATE TABLE new1 (
    n_id INT GENERATED ALWAYS AS IDENTITY,
    n_name VARCHAR NOT NULL
);


INSERT INTO new1 (n_name)
VALUES 
('Krunal'),
('Rahul');

ALTER TABLE new1
ADD COLUMN x_occupation VARCHAR(50);

ALTER TABLE new1
RENAME TO new2;

ALTER TABLE new2
DROP COLUMN n_occupation;

ALTER TABLE new2
RENAME COLUMN n_name TO name;

ALTER TABLE new2
ALTER COLUMN n_id TYPE int;

TRUNCATE TABLE new2;

CREATE TEMP TABLE krunaltemporery(
    temp_id INT
);

INSERT INTO krunaltemporery(temp_id)
VALUES (76586875675657);

DROP TABLE krunaltemporery;

CREATE TABLE krunal_copy AS
TABLE customer_japan;


-------------------------------Conditional Expressions & Operators-------------------------------------------
SELECT title,
       length,
       CASE
           WHEN length> 0
                AND length <= 50 THEN 'Short'
           WHEN length > 50
                AND length <= 120 THEN 'Medium'
           WHEN length> 120 THEN 'Long'
       END duration
FROM film
ORDER BY title;
--------------------------------------------------------------------------------------
SELECT
	COALESCE (NULL, 2 , 1);
--------------------------------------------------------------------------------------
SELECT
	NULLIF (1, 1); -- return NULL

SELECT
	NULLIF (1, 0); -- return 1

SELECT
	NULLIF ('A', 'B'); -- return A	
-----------------------------------------------------------------------------
SELECT
	CAST ('100' AS INTEGER);
	
-------------------------------Group by & Having-------------------------------------------
	
SELECT
	customer_id,
	SUM (amount)
FROM
	payment
GROUP BY
	customer_id
HAVING 
	SUM(amount)>100;
	
-------------------------------Joins------------------------------------------------

select first_name, last_name, email, phone, city_id, district
from customer c
INNER JOIN address a
ON c.address_id=a.address_id;
---------------------------------------------------------------------------
select first_name, last_name, email, phone, city_id, district
from customer c
FULL OUTER JOIN address a
ON c.address_id=a.address_id;
---------------------------------------------------------------------------------
SELECT f.film_id, title, inventory_id
FROM film f
LEFT JOIN inventory i
ON i.film_id = f.film_id;
--------------------------------------------------------------------------------
CREATE TABLE krunal (label CHAR(1) PRIMARY KEY);
CREATE TABLE suthar (score INT PRIMARY KEY);

INSERT INTO T1 (label) VALUES('A'),('B');

INSERT INTO T2 (score) VALUES(1),(2),(3);
	
SELECT * FROM krunal
CROSS JOIN suthar;
------------------------------------------------------------------
SELECT * 
FROM city
NATURAL JOIN country;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------PL/PG-SQL----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-------------------------------Dollar quoted string------------------------------------------------
select $msg$I'm a string constant that contains a backslash \$msg$;

-------------------------------Block structure------------------------------------------------
do $$ 
<<first_block>>
declare
  film_count integer := 0;
begin
   -- get the number of films
   select count(*) 
   into film_count
   from film;
   -- display a message
   raise notice 'The number of films is %', film_count;
end first_block $$;

-------------------------------Variable and constants------------------------------------------------
DO $$
DECLARE
	films top_rated_films%rowtype;
BEGIN 
	SELECT *
	FROM top_rated_films
	into films
	WHERE release_year = 1957;
	
	RAISE NOTICE '% - %', films.title, films.release_year;
END $$;
--------------------------------------------------------------------------------------

DO $$
DECLARE
	rec record;
BEGIN
	SELECT film_id, title, length , release_year
	INTO rec
	FROM film
	WHERE film_id = 100;
	
	RAISE NOTICE '% - % - %',rec.title, rec.length, rec.release_year;   	
END $$;

---------------------------------------------------------------------------------------------

DO $$
DECLARE
	pie CONSTANT NUMERIC = 3.14;
	r INTEGER = 7;
	result NUMERIC (10,2);
BEGIN
	SELECT pie * (POWER(r, 2)) INTO result;
	
	RAISE NOTICE 'Result - %', result;
END $$;

--------------------------------------Control Structure--------------------------------------------------------
do $$
declare
   v_film film%rowtype;
   len_description varchar(100);
begin  

  select * from film
  into v_film
  where film_id = 2;
  
  if not found then
     raise notice 'Film not found';
  else
      if v_film.length >0 and v_film.length <= 50 then
		 len_description := 'Short';
	  elsif v_film.length > 50 and v_film.length < 120 then
		 len_description := 'Medium';
	  elsif v_film.length > 120 then
		 len_description := 'Long';
	  else 
		 len_description := 'N/A';
	  end if;
    
	  raise notice 'The % film is %.',
	     v_film.title,  
	     len_description;
  end if;
end $$




do $$
declare 
	rate   film.rental_rate%type;
	price_segment varchar(50);
begin
    select rental_rate into rate 
    from film 
    where film_id = 100;
		if found then
		case rate
		   when 0.99 then
              price_segment =  'Low';
		   when 2.99 then
              price_segment = 'Medium';
		   when 4.99 then
              price_segment = 'High';
		   else
	    	  price_segment = 'Unspecified';
		   end case;
		raise notice '%', price_segment;  
    end if;
end; $$



do $$
declare
   n integer:= 10;
   fib integer := 0;
   counter integer := 0 ; 
   i integer := 0 ; 
   j integer := 1 ;
begin
	if (n < 1) then
		fib := 0 ;
	end if; 
	loop 
		exit when counter = n ; 
		counter := counter + 1 ; 
		select j, i + j into i,	j ;
	end loop; 
	fib := i;
    raise notice '%', fib; 
end; $$




do $$
declare 
   counter integer := 0;
begin
   while counter < 5 loop
      raise notice 'Counter %', counter;
	  counter := counter + 1;
   end loop;
end$$;


do
$$
declare
   counter int = 0;
begin
  loop
     counter = counter + 1;
	 exit when counter > 10;	 
	 continue when mod(counter,2) = 0;
	raise notice '%', counter;
  end loop;
end;
$$



do
$$
declare
    f record;
begin
    for f in select title, length 
	       from film 
	       order by length desc, title
	       limit 10 
    loop 
	raise notice '%(% mins)', f.title, f.length;
    end loop;
end;
$$



--------------------------------------Exception Handling--------------------------------------------------------
DO $$
DECLARE
	rec record;
	v_film_id int = 2000;
BEGIN
	SELECT film_id, title 
	INTO STRICT rec
	FROM film
	WHERE film_id = v_film_id;
	
	EXCEPTION
	   WHEN no_data_found THEN
	      RAISE EXCEPTION 'film % %not found', v_film_id;
END $$;

----------------------------------------------Stored procedures------------------------

CREATE OR REPLACE PROCEDURE transfer(sender INT, receiver INT, amt INT)
LANGUAGE PLPGSQL
AS $$
BEGIN
	UPDATE accounts
	SET balance = balance - amt
	WHERE id = sender;
	
	UPDATE accounts
	SET balance = balance + amt
	WHERE id = receiver;
	
	COMMIT;
END $$;

CALL transfer(2, 1, 1000);

DROP PROCEDURE transfer(INT, INT, INT);

---------------------------------------------------- Cursors -------------------------------------

CREATE OR REPLACE FUNCTION get_film_titles(p_year INT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
	titles TEXT DEFAULT '';
	rec record;
	cur_films CURSOR(p_year INT)
			FOR SELECT title, release_year
				FROM film
				WHERE release_year = p_year;
BEGIN
	OPEN cur_films(p_year);
	
	LOOP
		FETCH cur_films into rec;
		
		EXIT WHEN NOT FOUND;
		
		IF rec.title ILIKE '%ful%' THEN 
			IF titles != '' THEN
				titles = titles || ', ' || rec.title || ' - ' || rec.release_year;
			ELSE
				titles = rec.title || ' - ' || rec.release_year;
			END IF;
		END IF;
	END LOOP;
	
	CLOSE cur_films;
	
	RETURN titles;
END $$;

SELECT get_film_titles(2006) AS films;

---------------------------------------------------- Triggers -------------------------------------

DROP TABLE IF EXISTS employees;

CREATE TABLE employees(
   id INT GENERATED ALWAYS AS IDENTITY,
   first_name VARCHAR(40) NOT NULL,
   last_name VARCHAR(40) NOT NULL,
   PRIMARY KEY(id)
);

DROP TABLE IF EXISTS employee_audits
CREATE TABLE employee_audits (
   id INT GENERATED ALWAYS AS IDENTITY,
   employee_id INT NOT NULL,
   last_name VARCHAR(40) NOT NULL,
   changed_on TIMESTAMP(6) NOT NULL
);

CREATE OR REPLACE FUNCTION log_last_name_changes()
  RETURNS TRIGGER 
  LANGUAGE PLPGSQL
  AS
$$
BEGIN
	IF NEW.last_name <> OLD.last_name THEN
		 INSERT INTO employee_audits(employee_id,last_name,changed_on)
		 VALUES(OLD.id,OLD.last_name,now());
	END IF;

	RETURN NEW;
END;
$$

CREATE TRIGGER last_name_changes
  BEFORE UPDATE
  ON employees
  FOR EACH ROW
  EXECUTE PROCEDURE log_last_name_changes();
  
INSERT INTO employees (first_name, last_name)
VALUES ('krunal', 'suthar');

INSERT INTO employees (first_name, last_name)
VALUES ('arjun', 'gadani');

delete from employees
where id=3;
SELECT * FROM employees;

UPDATE employees
SET last_name = 'KKKRRRUUUNNNAAALLL'
WHERE ID = 1;
SELECT * FROM employee_audits;


---------------------------------------------------- User Defined functions -------------------------------------


create function get_film_count(len_from int, len_to int)
returns int
language plpgsql
as
$$
declare
   film_count integer;
begin
   select count(*) 
   into film_count
   from film
   where length between len_from and len_to;
   
   return film_count;
end;
$$;

DROP FUNCTION get_film_count;




create or replace function get_film_stat(
    out min_len int,
    out max_len int,
    out avg_len numeric) 
language plpgsql
as $$
begin
  
  select min(length),
         max(length),
		 avg(length)::numeric(5,1)
  into min_len, max_len, avg_len
  from film;

end;$$






create or replace function swap(
	inout x int,
	inout y int
) 
language plpgsql	
as $$
begin
   select x,y into y,x;
end; $$;





create or replace function get_film (
  p_pattern varchar
) 
	returns table (
		film_title varchar,
		film_release_year int
	) 
	language plpgsql
as $$
begin
	return query 
		select
			title,
			release_year::integer
		from
			film
		where
			title ilike p_pattern;
end;$$


select * from get_film('a%')

drop function get_film(varchar);