drop function if exists openarc.schemamigrate(); create or replace function openarc.schemamigrate() returns int as $$ declare ret int;

declare major  varchar(3)  := '001';
declare minor  varchar(4)  := '0001';
declare secrel varchar(4)  := '0000';
declare script varchar(50) := 'openarc.001.0001.0000.sql';

begin
if openarc.schema_check( major, minor, secrel ) = 0 then

drop table if exists openarc.rpc_registry;
create table openarc.rpc_registry(
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

perform openarc.schema_control_update( major, minor, secrel, script );
end if; select openarc.schema_check( major, minor, secrel ) into ret; return ret; end; $$ language plpgsql; select openarc.schemamigrate();