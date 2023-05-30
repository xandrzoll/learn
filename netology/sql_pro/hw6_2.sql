create database stores;

-- server -- db stores
CREATE TABLE inventory_store_01 (
	inventory_id serial4 NOT NULL,
	film_id int2 NOT NULL,
	store_id int2 NOT null check (store_id = 1),
	last_update timestamp NOT NULL DEFAULT now()
);
CREATE INDEX inventory_store_01_idx ON inventory_store_01(inventory_id);

CREATE TABLE inventory_store_02 (
	inventory_id serial4 NOT NULL,
	film_id int2 NOT NULL,
	store_id int2 NOT null check (store_id = 2),
	last_update timestamp NOT NULL DEFAULT now()
);
CREATE INDEX inventory_store_02_idx ON inventory_store_02(inventory_id);
------------

-- main database
create extension postgres_fdw; 

create server inventory_store_server
foreign data wrapper postgres_fdw
options (host'localhost', port'5432', dbname'stores');

create user mapping for postgres
	server inventory_store_server
	options (user'postgres', password'1qwe');

CREATE foreign TABLE inventory_store_01_out (
	inventory_id serial4 NOT NULL,
	film_id int2 NOT NULL,
	store_id int2 NOT null check (store_id = 1),
	last_update timestamp NOT NULL DEFAULT now()
)
inherits(inventory)
server inventory_store_server
options (schema_name'public', table_name'inventory_store_01')

CREATE foreign TABLE inventory_store_02_out (
	inventory_id serial4 NOT NULL,
	film_id int2 NOT NULL,
	store_id int2 NOT null check (store_id = 2),
	last_update timestamp NOT NULL DEFAULT now()
)
inherits(inventory)
server inventory_store_server
options (schema_name'public', table_name'inventory_store_02')


create or replace function inventory_insert_db_tg() returns trigger as $$
begin 
	if new.store_id = 1 then 
		insert into inventory_store_01_out values (new.*);
	elseif new.store_id = 2 then
		insert into inventory_store_02_out values (new.*);
	else raise exception 'Отсутствует партиция';
	end if;
return null;
end;
$$ language plpgsql;

create trigger inventory_insert_db_tg
before insert on inventory
for each row execute function inventory_insert_db_tg();

create temporary table inventory_tmp as (select * from inventory);
delete from inventory;
insert into inventory select * from inventory_tmp;

select * from inventory;
select * from only inventory;
select * from inventory_store_01_out;
select * from inventory_store_02_out;
