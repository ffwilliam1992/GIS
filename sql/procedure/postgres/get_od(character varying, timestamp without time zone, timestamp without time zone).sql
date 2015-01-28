-- Function: taxi.get_od(character varying, timestamp without time zone, timestamp without time zone)

-- DROP FUNCTION taxi.get_od(character varying, timestamp without time zone, timestamp without time zone);

CREATE OR REPLACE FUNCTION taxi.get_od(
    v_uid character varying,
    v_begin timestamp without time zone,
    v_end timestamp without time zone DEFAULT now())
  RETURNS setof record AS
$BODY$
begin
	return query
	select
		T1.id, T1.point, T1.timestamp, T2.point, T2.timestamp
	from (
		select
			row_number() over(), *
		from (
			select
				lead(state) over w,
				lag(state) over w,
				state,
				point, id,
				st_x(point) longitude,
				st_y(point) latitude,
				timestamp
			from
				taxi.gps_raw
			where
				timestamp between v_begin and  v_end and
				id = v_uid
			window w as (
				order by
					timestamp
			)
		)as T
    where
        lead and
        not lag and
        state
    )as T1, (
    select
        row_number() over(), *
    from (
        select
            lead(state) over w,
            lag(state) over w,
            state,
            point, id,
            st_x(point) longitude,
            st_y(point) latitude,
            timestamp
        from
            taxi.gps_raw
        where
            timestamp between v_begin and v_end and
            id = v_uid
        window w as (
            order by
                timestamp
        )
    )as T
    where
        not lead and
        lag and
        state
    )as T2
	where
		T1.lead and
		not T1.lag and
		T1.state and
		not T2.lead and
		T2.lag and
		T2.state and
		T1.row_number = T2.row_number
;

end;
$BODY$
  LANGUAGE plpgsql IMMUTABLE STRICT
  COST 100;
ALTER FUNCTION taxi.get_od(character varying, timestamp without time zone, timestamp without time zone)
  OWNER TO tx;
