create database homeworks;

-- psql -h myhostname -p 49245 -U postgres -d homeworks < /home/zaron/Downloads/hr.sql

drop role if exists MyUser;
create role MyUser with login;

revoke all privileges on database homeworks from MyUser;
revoke all privileges on schema public from MyUser;
revoke all privileges on schema pg_catalog from MyUser;
revoke all privileges on schema information_schema from MyUser;

revoke all privileges on database homeworks from public;
revoke all privileges on schema public from public;
revoke all privileges on schema pg_catalog from public;
revoke all privileges on schema information_schema from public;

grant connect on database homeworks to MyUser;
grant usage on schema hr to MyUser;
grant select on table hr.projects to MyUser;
grant select on table hr."structure" to MyUser;

alter role MyUser password '12345';
alter role MyUser valid until '2022-12-31';

-- psql -h myhostname -p 49245 -U myuser -d homeworks
-- select * from hr.projects

revoke select on table hr.projects from MyUser;
revoke select on table hr."structure" from MyUser;

drop role if exists MyUser;


begin;
insert into hr.city (city_id, city) values (14, 'Moscow');
savepoint point_0;
delete from hr.city where city_id = 14;
rollback to point_0;
commit;