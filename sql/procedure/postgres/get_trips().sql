-- Function: taxi.get_trips(character varying)

-- DROP FUNCTION taxi.get_trips(character varying);
--This procedure filles the field trip_id of taxi.gps_raw
CREATE OR REPLACE FUNCTION taxi.get_trips()
  RETURNS void AS
$BODY$
declare v_record record;
declare v_pre_state boolean default false;
declare v_trip_id bigint default 0;
declare	v_cur no scroll cursor for
		select 
			id, state, timestamp
		from
			taxi.gps_raw
		order by
			id, timestamp
	for update
;
begin
	for v_record in v_cur
	loop
		--continuous uncarried
		if(not v_pre_state and not v_record.state) then
			continue;
		end if;
		--carried to uncarried
		if(v_pre_state and not v_record.state) then
			v_pre_state = false;
			continue;
		end if;
		--uncarried to carried
		if(not v_pre_state and v_record.state) then
			v_trip_id = v_trip_id + 1;
			v_pre_state = true;
		end if;

		update 
			taxi.gps_raw
		set 
			trip_id = v_trip_id
		where
			current of v_cur
		;
	end loop;
end;
$BODY$
  LANGUAGE plpgsql
  COST 100
;
ALTER FUNCTION taxi.get_trips()
  OWNER TO tx;