create or replace function save_salary_history() returns trigger 
as $$
	begin 
		if TG_OP = 'INSERT' then 
			if NEW.emp_id in (select distinct emp_id from employee_salary) then
				raise exception
					'The employee is here';
				return Null;
			else
				insert into employee_salary_history(emp_id, salary, start_date)
					values (NEW.emp_id, NEW.salary, NEW.effective_from);
			end if;
		elsif TG_OP = 'UPDATE' then
			update employee_salary_history
			set end_date = NEW.effective_from - '1 day'::interval
			where emp_id = NEW.emp_id
				and start_date = (select max(start_date) from employee_salary_history where emp_id = NEW.emp_id);
				
			insert into employee_salary_history(emp_id, salary, start_date)
					values (NEW.emp_id, NEW.salary, NEW.effective_from);
		end if;
		return NEW;
	end;
$$ language plpgsql;


create trigger change_salary
after insert or update on employee_salary
for each row execute procedure save_salary_history();