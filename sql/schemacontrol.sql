-- create schema control object
drop schema if exists openlibarc cascade;
create schema openlibarc;

create table openlibarc.schemacontrol (
    id serial,
    major_release varchar(3) not null,
    minor_release varchar(4) not null,
    sec_release varchar(4) not null,
    install_script varchar(50) not null,
    date_applied timestamp not null
);

alter table openlibarc.schemacontrol
    add constraint schemacontrol_pk
        primary key ( major_release, minor_release, sec_release );

drop function if exists openlibarc.schema_check( varchar, varchar, varchar);
create or replace function openlibarc.schema_check(
    major  varchar(3),
    minor  varchar(4),
    secrel varchar(4)
)
returns int as $$
declare ret int;
begin
    select count(*) into ret
    from   openlibarc.schemacontrol
    where  major_release=major
    and    minor_release=minor
    and    sec_release=secrel;
    return ret;
end; $$ language plpgsql;

drop function if exists openlibarc.schema_control_update( varchar, varchar, varchar, varchar );
create or replace function openlibarc.schema_control_update(
    major  varchar(3),
    minor  varchar(4),
    secrel varchar(4),
    script varchar(50)
)
returns void as $$
begin
    insert into openlibarc.schemacontrol
        ( major_release, minor_release, sec_release, install_script, date_applied )
    values
        ( major, minor, secrel, script, current_timestamp );
end; $$ language plpgsql;
