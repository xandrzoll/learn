-- Задание 1 -----------------------------------------	
create or replace function get_vacancy_count(vacancy_name text, dt_start date default '1990-01-01', dt_end date default now(), OUT vacancy_count numeric)
as $$
	begin 
		if dt_start > dt_end then
			raise exception 
				'Start date must be less then end date. '
				'Start date is %. '
				'End date is %', dt_start, dt_end;
		end if;
		select count(*) from hr.vacancy
		where 1 = 1
			and vac_title = vacancy_name
			and create_date >= dt_start
			and (closure_date <= dt_end or closure_date is Null)
		into vacancy_count;
	end;
$$ language plpgsql;

select get_vacancy_count('стажер', '2017-01-01');
--------------------------------------------------------

-- Задание 2 -------------------------------------------
create or replace function check_grade() returns trigger 
as $$
	begin 
		if NEW.grade not in (select distinct grade from hr.grade_salary) then
			raise exception
				'Position grade is not present in table "grade_salary"';
			return null;
		end if;
		return NEW;
	end;
$$ language plpgsql;

drop trigger if exists insert_new_position on hr."position";

create trigger insert_new_position
before insert on hr."position"
for each row execute procedure check_grade();

insert into hr."position"
(pos_id, pos_title, pos_category, unit_id, grade, address_id, manager_pos_id)
values(4593, 'test_title_0', 'test_category_0', 45, 55, 7, 1145);

insert into hr."position"
(pos_id, pos_title, pos_category, unit_id, grade, address_id, manager_pos_id)
values(4594, 'test_title_1', 'test_category_1', 45, 55, 7, 1145);

delete from hr."position"
where pos_id > 4591;
--------------------------------------------

-- Задание 3 -------------------------------

create table hr.employee_salary_history(
	emp_id int4,
	salary_old numeric(12, 2),
	salary_new numeric(12, 2),
	difference numeric(12, 2),
	last_update date
);

create or replace function save_salary_history() returns trigger 
as $$
	begin 
		if NEW.emp_id in (select distinct emp_id from hr.employee_salary_history) then
			update hr.employee_salary_history
			set
				salary_new = NEW.salary,
				salary_old = OLD.salary,
				difference = NEW.salary - OLD.salary,
				last_update = NOW()
			where emp_id = NEW.emp_id;
		else
			insert into hr.employee_salary_history(emp_id, salary_old, salary_new, difference, last_update)
			values (NEW.emp_id, 0, NEW.salary, 0, NEW.effective_from);
		end if;
		return NEW;
	end;
$$ language plpgsql;

drop trigger if exists change_salary on hr.employee_salary;

create trigger change_salary
after insert or update of salary on hr.employee_salary
for each row execute procedure save_salary_history();


insert into hr.employee_salary(order_id, emp_id, salary, effective_from)
values (29969, 356, 10000, '2022-12-02');

update hr.employee_salary
set 
	effective_from  = '2022-12-22'
where order_id = 29968;

select * from hr.employee_salary_history;
---------------------------------------------

-- Задание 4 --------------------------------
create or replace procedure insert_employee_salary(
	emp_id int4, salary numeric(12, 2), effective_from date)
as $$
	declare last_id int4; 
	begin 
		select max(order_id)+1 from hr.employee_salary into last_id;
		insert into hr.employee_salary("order_id", "emp_id", "salary", "effective_from")
			values (last_id, emp_id, salary, effective_from);
		if salary > 100000 then -- it's too much for my workers!!!
			rollback;
		else
			commit;
		end if;
	
	end;
$$ language plpgsql;

call insert_employee_salary(1, 10000, '2022-12-20');
call insert_employee_salary(1, 100001, '2022-12-20');

select * from hr.employee_salary where emp_id = 1 ;
















