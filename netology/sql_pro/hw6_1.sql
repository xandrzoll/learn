create or replace function partition_inventory(store_id int) returns void
as $$
declare 
	partition_table_name text;
begin 
	partition_table_name = concat('inventory_store_', lpad(store_id::text, 2, '0'));
	if (to_regclass(partition_table_name) is not null) then
		raise exception 'Такая партиция уже существует.';
		return;
	end if;
	-- create
	execute format(
		'create table %I (check (store_id = %L)) inherits (inventory);', 
		partition_table_name, store_id
	);
	execute format(
		'create index %1$s_idx on %1$s(inventory_id)', 
		partition_table_name
	);
	-- transfer data to partition
	execute format(
		'with cte as '
		'(delete from only inventory where store_id = %2$L returning *) '
		'insert into %1$I '
		'select * from cte;', 
		partition_table_name, store_id
	);
--	-- rule for inserting
	execute format(
		'create rule %1$s_inserting as on insert to inventory '
		'where (store_id = %2$L) '
		'do instead insert into %1$s values (new.*)', 
		partition_table_name, store_id
	);
	-- rule for updating
	execute format(
		'create rule %1$s_updating as on update to inventory '
		'where (new.store_id != %2$L and old.store_id = %2$L) '
		'do instead (insert into inventory values (new.*); delete from %1$s where inventory_id=new.inventory_id)', 
		partition_table_name, store_id
	);
	return;
end;
$$ language plpgsql;

select partition_inventory(1);
select partition_inventory(2);

-- tests
select * from inventory
where store_id = 1;
select * from only inventory
where store_id = 1;

update inventory 
set last_update = '2023-01-12 05:09:17.000'
where inventory_id = 1

select * from inventory
where inventory_id = 1;

select * from inventory_store_01
where inventory_id = 1;

update inventory 
set store_id = 2
where inventory_id = 1

select * from inventory_store_01
where inventory_id = 1;
select * from inventory_store_02
where inventory_id = 1;

delete from inventory 
where inventory_id = 1;

select * from inventory
where inventory_id = 1;


