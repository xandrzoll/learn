select * from pg_available_extensions;

create extension postgres_fdw;


-- Задание 1 ---------------------------
---- create role on server, add mapping on /etc/postgresql/14/main/pg_hba.conf
--create role foreign_user with encrypted password 'foreign_password';
--alter role foreign_user login;
--revoke all privileges on schema public from foreign_user;
--revoke all privileges on schema pg_catalog from foreign_user;
--revoke all privileges on schema information_schema from foreign_user;
--revoke all privileges on schema hr from foreign_user;
--grant connect on database homeworks to foreign_user;
--grant usage on schema hr to foreign_user;
--grant select on table hr.employee to foreign_user;
--grant select on table hr."position" to foreign_user;

create server klepa_one_server
	foreign data wrapper postgres_fdw
	options(
		host'ovz1.9200551313.wmekm.vps.myjino.ru',
		port'49245',
		dbname'homeworks'
	);
	
create user mapping for postgres
	server klepa_one_server
	options (
		user'foreign_user',
		password'foreign_password'
	);
	
create foreign table out_employee (
	emp_id int4 NOT NULL,
	person_id int4 NOT NULL,
	pos_id int4 NOT NULL
)
server klepa_one_server
options (
	schema_name'hr',
	table_name'employee'
);

create foreign table out_position (
	pos_id int4 NOT NULL,
	pos_title varchar(250) NOT NULL,
	pos_category varchar(100) NULL,
	unit_id int4 NULL
)
server klepa_one_server
options (
	schema_name'hr',
	table_name'position'
);

select emp_id, person_id, e.pos_id, pos_title, pos_category, unit_id
from out_employee e
join out_position p on e.pos_id = p.pos_id;
-----------------------------------------------------

-- Задание 2 ---------------------------------------
create extension tablefunc;

select * from crosstab (
	$$
	select yy, coalesce(dt, 'total'), s
	from (
		select extract(year from created_at)::text yy, lpad(extract(month from created_at)::text, 2, '0') dt, sum(amount) s from projects
		group by cube(1, 2)
		order by 1, 2
	) t
	$$,
	$$
	(select distinct lpad(extract(month from created_at)::text, 2, '0') from projects
	order by 1)
	union all 
	select 'total'
	$$
)
AS cst("Год" numeric, "Январь" numeric, "Февраль" numeric, "Март" numeric, "Апрель" numeric, "Май" numeric, "Июнь" numeric, 
	"Июль" numeric, "Август" numeric, "Сентябрь" numeric, "Октябрь" numeric, "Ноябрь" numeric, "Декабрь" numeric, "Итого" numeric);
-----------------------------------------------------

-- Задание 3 --------------------------------------------------------------------------------------
create extension pg_stat_statements;

SELECT userid, query, calls, total_exec_time, min_exec_time, max_exec_time, stddev_exec_time, "rows"
FROM pg_stat_statements
where query like '%from hr.employee%'














