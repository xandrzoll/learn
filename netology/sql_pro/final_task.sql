create role netocourier with login password 'NetoSQL2022' valid until '2023-04-01';
drop role netocourier;

revoke all privileges on schema public from netocourier;
revoke all privileges on schema pg_catalog from netocourier;
revoke all privileges on schema information_schema from netocourier;

grant connect on database postgres to netocourier;
grant usage on schema public TO netocourier;
grant usage on schema pg_catalog TO netocourier;
grant usage on schema information_schema TO netocourier;
grant usage on schema fake_data TO netocourier;

grant select, insert, update, delete, truncate on all tables in schema public to netocourier;
grant all privileges on schema public to netocourier;

grant select on all tables in schema pg_catalog to netocourier;
grant select on all tables in schema information_schema to netocourier;
grant select on all tables in schema fake_data to netocourier;


SELECT * FROM pg_available_extensions

CREATE TYPE status_enum AS ENUM ('В очереди', 'Выполняется', 'Выполнено', 'Отменен');

CREATE TABLE courier (
    id uuid DEFAULT uuid_generate_v4(),
    from_place varchar NOT NULL,  	--откуда
    where_place varchar NOT NULL, 	--куда
    "name" varchar NOT NULL,		--название документа
	account_id uuid,				--FK id контрагента
	contact_id uuid, 				--FK id контакта 
	description text, 				--описание
	user_id uuid,   				--FK id сотрудника отправителя
	status status_enum default 'В очереди', 	--статусы 
	created_date date default now(), --дата создания заявки, значение по умолчанию now()

    constraint courier_id PRIMARY KEY (id)
);

CREATE TABLE account (
    id uuid DEFAULT uuid_generate_v4(),
    name VARCHAR NOT NULL,  	--название контрагента
    constraint account_id PRIMARY KEY (id)
);

CREATE TABLE contact (
    id uuid DEFAULT uuid_generate_v4(),
    last_name VARCHAR NOT NULL,  	--фамилия контакта
    first_name VARCHAR NOT NULL, 	--имя контакта
	account_id uuid,				--FK id контрагента

    constraint contact_id PRIMARY KEY (id)
);

CREATE TABLE "user" (
    id uuid DEFAULT uuid_generate_v4(),
    last_name VARCHAR NOT asNULL,  	
    first_name VARCHAR NOT NULL,sdsasadasdas
	dismissed boolean default false,

    constraint user_id PRIMARY KEY (id)asddsa
);

ALTER TABLE courier ADD CONSTRAINT courier_account_id_fkey FOREIGN KEY (account_id) REFERENCES account(id);
ALTER TABLE courier ADD CONSTRAINT courier_contact_id_fkey FOREIGN KEY (contact_id) REFERENCES contact(id);
ALTER TABLE courier ADD CONSTRAINT courier_user_id_fkey FOREIGN KEY (user_id) REFERENCES "user"(id);
ALTER TABLE contact ADD CONSTRAINT contact_account_id_fkey FOREIGN KEY (account_id) REFERENCES account(id);


create or replace procedure insert_test_data(value int default 1)
as $$
declare
	random_male bool;
	random_first_name varchar;
	random_last_name varchar;
	random_account_name varchar;
	random_account_id uuid;
	random_city varchar;
	random_address_from varchar;
	random_address_to varchar;
	random_doc_name varchar;
	random_contact_id uuid;
	random_user_id uuid;
	random_status status_enum;
	random_date date;
begin
	for i in 1..value loop
		random_male := (values (true), (false) order by random() limit 1);
		random_first_name := (select name from fake_data.first_names where male = random_male order by random() limit 1);
		random_last_name := (select name from fake_data.last_names where male = random_male order by random() limit 1);
		random_account_name := (select company_name from fake_data.companies order by random() limit 1);
		insert into public."user" (first_name, last_name) values (random_first_name, random_last_name);
		insert into public."account" (name) values (random_account_name);
	end loop;

	for i in 1..value*2 loop
		random_male := (values (true), (false) order by random() limit 1);
		random_first_name := (select name from fake_data.first_names where male = random_male order by random() limit 1);
		random_last_name := (select name from fake_data.last_names where male = random_male order by random() limit 1);
		random_account_id := (select id from account order by random() limit 1);
		insert into public.contact (first_name, last_name, account_id) values (random_first_name, random_last_name, random_account_id);
	end loop;

	for i in 1..value*5 loop
		random_city := (select city_name from fake_data.cities order by random() limit 1);
		random_address_from := random_city || ', ' 
		|| (select street_name from fake_data.streets order by random() limit 1) || ', ' 
		|| (select building_number from fake_data.buildings order by random() limit 1);
		random_address_to := random_city || ', ' 
		|| (select street_name from fake_data.streets order by random() limit 1) || ', ' 
		|| (select building_number from fake_data.buildings order by random() limit 1);
		random_doc_name := (select repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',(random()*33)::integer,
							(random()*33)::integer), (random()*10)::integer)::varchar(50));
		random_account_id := (select id from account order by random() limit 1);
		random_contact_id := (select id from contact order by random() limit 1);
		random_user_id := (select id from "user" order by random() limit 1);
		random_status := (select (enum_range(null::status_enum))[floor(random() * 4 + 1)]);
		random_date := (select now() - interval '1 day' * round(random() * 1000) as timestamp);
		insert into public.courier (from_place, where_place, "name", account_id, contact_id, user_id, status, created_date) 
			values (random_address_from, random_address_to, random_doc_name, random_account_id, random_contact_id, 
					random_user_id, random_status, random_date);
	end loop;
end
$$ language plpgsql;

create or replace procedure erase_test_data()
as $$
begin 
	truncate public.account cascade;
	truncate public.contact cascade;
	truncate public.courier cascade;
	truncate public."user" cascade;
end
$$ language plpgsql;

create or replace procedure add_courier(from_place varchar, where_place varchar, name varchar, account_id uuid, contact_id uuid, description text, user_id uuid)
as $$
begin 
	insert into public.courier ("from_place", "where_place", "name", "account_id", "contact_id", "description", "user_id") 
		values (add_courier.from_place, add_courier.where_place, add_courier."name", add_courier.account_id, add_courier.contact_id, add_courier.description, add_courier.user_id);
end
$$ language plpgsql;

create or replace function add_courier(from_place varchar, where_place varchar, name varchar, account_id uuid, contact_id uuid, description text, user_id uuid) returns boolean
as $$
begin 
	insert into public.courier ("from_place", "where_place", "name", "account_id", "contact_id", "description", "user_id") 
		values (add_courier.from_place, add_courier.where_place, add_courier."name", add_courier.account_id, add_courier.contact_id, add_courier.description, add_courier.user_id);
	return true;
end
$$ language plpgsql;

select add_courier('ascac', 'asadad', 'saddasdads', '7f26a542-90b5-43cb-a0ab-65ccedfd3a29', '807486a6-506d-4cce-bbe1-5a538f0b8175', 'ascascac', 'cace204a-fd9e-4c52-9ec6-c26664e3204a')

create or replace function get_courier() returns table (
	id uuid,
    from_place varchar,
    where_place varchar,
    "name" varchar,
	account_id uuid,
	account varchar,
	contact_id uuid,
	contact varchar,
	description text,
	user_id uuid,
	"user" varchar,
	status status_enum,
	created_date date
)
as $$
begin 
	return query
		select 
			  c.id
			, c.from_place
			, c.where_place
			, c.name
			, c.account_id
			, a.name as "account"
			, c.contact_id
			, (coalesce(c2.first_name, '') || ' ' || coalesce(c2.last_name, ''))::varchar(150) as "contact"
			, c.description
			, c.user_id
			, (coalesce(u.first_name, '') || ' ' || coalesce(u.last_name, ''))::varchar(150) as "user"
			, c.status 
			, c.created_date 
		from public.courier c 
			left join public.account a 
				on c.account_id = a.id
			left join public.contact c2 
				on c.contact_id = c2.id 
			left join public."user" u 
				on c.user_id = u.id 
		--where c.user_id = 'dffe0eba-4e04-46c1-8ec1-9214551bdf42'
		order by c.user_id, c.status, c.created_date desc
	;
end;
$$ language plpgsql;


create or replace procedure change_status(new_status status_enum, id uuid)
as $$
begin 
	update public.courier c
	set status = new_status
	where c.id = change_status.id;
end;
$$  language plpgsql;


create or replace function get_users() returns table (
	"user" varchar
)
as $$
begin 
	return query
		select (coalesce(last_name, '') || ' ' || coalesce(first_name, ''))::varchar(150) as "user"
		from public."user"
		where dismissed = false
		order by last_name
	;
end;
$$ language plpgsql;


create or replace function get_accounts() returns table (
	account varchar
)
as $$
begin 
	return query
		select name as "account"
		from public.account
		order by name
	;
end;
$$ language plpgsql;


create or replace function get_contacts(account_id uuid) returns table (
	contact varchar
)
as $$
begin 
	if account_id is null or account_id not in (select id from account) then
		return query select 'Контрагент не найден'::varchar as contact;
	end if;
	return query
		select 
			(coalesce(last_name, '') || ' ' || coalesce(first_name, ''))::varchar(150) as "contact" 
		from public.contact c 
		where id in (select contact_id from courier c0 where c0.account_id = get_contacts.account_id)
		order by last_name;
end;
$$ language plpgsql;


create or replace view courier_statistic as 
select 
	  t0.*
	, t1.count_where_place
	, t2.count_contact
	, t3.cancel_user_array
from (
	select 
		  account_id
		, a.name as "account"
		, count(*) as count_courier
		, sum(case when status = 'Выполнено' then 1 else 0 end) as count_complete
		, sum(case when status = 'Отменен' then 1 else 0 end) as count_canceled
		, coalesce(sum(case when date_trunc('month', created_date) = date_trunc('month', now() - interval'1' month) then 1.0 else 0.0 end)::numeric/ 
		  nullif(sum(case when date_trunc('month', created_date) = date_trunc('month', now()) then 1.0 else 0.0 end)::numeric, 0) - 1.0, 0.0)*100
			as percent_relative_prev_month
	from public.courier c
		left join public.account a on c.account_id = a.id
	group by c.account_id, a.name
) as t0
left join (
	select account_id, count(*) as count_where_place from (
		select distinct 
			  account_id
			, where_place
		from public.courier
	) t_where_place 
	group by account_id
) as t1 on t0.account_id = t1.account_id
left join (
	select account_id, count(*) as count_contact from (
		select distinct 
			  account_id
			, contact_id
		from public.courier
	) t_contact 
	group by account_id
) as t2 on t0.account_id = t2.account_id
left join (
	select account_id, array_agg(user_id)  as cancel_user_array
	from public.courier
	where status = 'Отменен'
	group by account_id
) as t3 on t0.account_id = t3.account_id


drop table rtim_test;
CREATE TABLE rtim_trase (
    msisdn bigint, 	-- id клиента
    inp_id int,  	-- id дерева
    clip_id int NOT NULL, 	--id точки отсечения
    date_event timestamp,	-- время события
	clip_value json
);

CREATE TABLE rtim_inp (
    id int,  	-- id дерева
    description  varchar, 	--id точки отсечения
    
    date_event timestamp,	-- время события
	clip_value json
);


CREATE TABLE rtim_clip (
    clip_id int NOT NULL, 	--id точки отсечения
    last_update timestamp,	-- время события
	rule_id int,
	
);




insert into rtim_test (msisdn, inp_id, clip_id, date_event, clip_value) values (
	79200551332, 12345,	123654790, TIMESTAMP '2023-04-27 10:23:54+02',
	'{"tag_name_1": 10, "tag_name_2": 11,"tag_name_3": 12,"tag_name_4": 13,"tag_name_5": 14,"tag_name_6": 15,"tag_name_7": 16,"tag_name_8": 17}'
)

select *
from information_schema.tables 
where table_schema = 'public'

SELECT pg_size_pretty( pg_total_relation_size( 'public.rtim_test' ) );
8192 b
16kB
48 kB

32kB

select count(*) from rtim_test




