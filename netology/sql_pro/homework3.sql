CREATE TABLE settlement (
	settlement_id int4 NOT NULL,
	settlement varchar(50) NOT NULL,
	CONSTRAINT settlement_pkey PRIMARY KEY (settlement_id)
);

CREATE TABLE address (
	address_id int4 NOT NULL,
	settlement_id int4 NOT NULL,
	street varchar(100),
	house varchar(20),
	CONSTRAINT address_pkey PRIMARY KEY (address_id)
);
ALTER TABLE address ADD CONSTRAINT address_settlement_id_fkey FOREIGN KEY (settlement_id) REFERENCES settlement(settlement_id);

CREATE TABLE person (
	person_id uuid NOT NULL default gen_random_uuid(),
	name varchar(250) NOT NULL,
	dob date NULL,
	address_id int4 NULL,
	CONSTRAINT person_pkey PRIMARY KEY (person_id)
);
ALTER TABLE person ADD CONSTRAINT person_address_id_fkey FOREIGN KEY (address_id) REFERENCES address(address_id);

CREATE TABLE email (
	email_id uuid NOT NULL default gen_random_uuid(),
	email varchar(100) NOT NULL,
	person_id uuid NOT NULL,
	CONSTRAINT email_pkey PRIMARY KEY (email_id)
);
ALTER TABLE email ADD CONSTRAINT email_person_id_fkey FOREIGN KEY (person_id) REFERENCES person(person_id);



CREATE TABLE employee (
	emp_id uuid NOT NULL default gen_random_uuid(),
	person_id uuid NOT NULL,
	pos_id int4 NOT NULL,
	start_date date NOT NULL,
	end_date date NULL,
	CONSTRAINT employee_pkey PRIMARY KEY (emp_id)
);

CREATE TABLE "position" (
	pos_id int4 NOT NULL,
	pos_name varchar(255) NOT NULL,
	dep_id int4 NULL,
	CONSTRAINT position_pkey PRIMARY KEY (pos_id)
);

CREATE TABLE "structure" (
	dep_id int4 NOT NULL,
	dep_name varchar(255),
	address_id int4 NULL,
	boss_id int4 NULL,
	CONSTRAINT structure_pkey PRIMARY KEY (dep_id)
);

CREATE TABLE employee_salary (
	order_id int4 NOT NULL,
	emp_id uuid NOT NULL,
	salary numeric(12, 2) NOT NULL,
	effective_from date NOT NULL,
	CONSTRAINT employee_salary_pkey PRIMARY KEY (order_id)
);

ALTER TABLE employee ADD CONSTRAINT employee_person_id_fkey FOREIGN KEY (person_id) REFERENCES person(person_id);
ALTER TABLE employee ADD CONSTRAINT employee_pos_id_fkey FOREIGN KEY (pos_id) REFERENCES "position"(pos_id);

ALTER TABLE employee_salary ADD CONSTRAINT employee_salary_emp_id_fkey FOREIGN KEY (emp_id) REFERENCES employee(emp_id);

ALTER TABLE "position" ADD CONSTRAINT position_dep_id_fkey FOREIGN KEY (dep_id) REFERENCES "structure"(dep_id);

ALTER TABLE "structure" ADD CONSTRAINT structure_address_id_fkey FOREIGN KEY (address_id) REFERENCES address(address_id);
ALTER TABLE "structure" ADD CONSTRAINT structure_pos_boss_id_fkey FOREIGN KEY (boss_id) REFERENCES "position"(pos_id);


CREATE TABLE employee_salary_history (
	order_id int4 NOT NULL,
	emp_id uuid NOT NULL,
	salary numeric(12, 2) NOT NULL,
	start_date date NOT NULL,
	end_date date NULL
);

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




