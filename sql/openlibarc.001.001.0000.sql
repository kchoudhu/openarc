drop function if exists openlibarc.schemamigrate(); create or replace function openlibarc.schemamigrate() returns int as $$ declare ret int;

declare major  varchar(3)  := '001';
declare minor  varchar(4)  := '0001';
declare secrel varchar(4)  := '0000';
declare script varchar(50) := 'openlibarc.001.0001.0000.sql';

begin
if openlibarc.schema_check( major, minor, secrel ) = 0 then

drop table if exists openlibarc.rpc_registry;
create table openlibarc.rpc_registry(
    _rpc_id serial primary key,
    servicename varchar(30) not null,
    owning_class varchar(20) not null,
    owner_id int not null,
    role varchar(10) not null,
    runhost varchar(100) not null,
    connhost varchar(100) not null,
    connport int not null,
    heartbeat timestamp,
    unique(owning_class, owner_id, servicename, role)
);

perform openlibarc.schema_control_update( major, minor, secrel, script );
end if; select openlibarc.schema_check( major, minor, secrel ) into ret; return ret; end; $$ language plpgsql; select openlibarc.schemamigrate();