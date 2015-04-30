select id, timestamp , state from taxi.gps_raw
where
	--id = 'abboip' and
	timestamp >= '2008-05-16 22:51:10' and
	timestamp <=  '2008-05-18 22:51:10'
order by
	id, timestamp
limit 100;

select * from ( values(1, 2, 3))  as a (a, b, c);
select taxi.get_trips();

select id, count(*)
from 
	taxi.gps_raw
where 
	state
group by
	id
order by
	2
;
update taxi.gps_raw set trip_id=null where trip_id is not null
;

select *
from taxi.gps_raw_test
--where trip_id is not null
order by
	id, timestamp
limit 100;

select taxi.get_trips();

select taxi.get_trips_with_od(-122.419195,37.775057, -122.405891,37.785639, 100, 150)

truncate table taxi.gps_raw_test;
insert into taxi.gps_raw_test select * from taxi.gps_raw where id = 'abniar';
insert into taxi.gps_raw_test select * from taxi.gps_raw limit 50000;

select degrees(st_azimuth(st_point(0, 0), st_point(-sqrt(3), 1)));

select 
	T1.id, st_asText(the_geom)
from
	(select taxi.get_trip_vertices(198104, true) as id )T1,
	taxi.edges_vertices_pgr T2
where
	T1.id = T2.id
	
	;
	select
		st_asText(point)
	from
		taxi.gps_raw
	where
		trip_id = 1
	order by
		timestamp
select taxi.get_nearest_vertex(st_point(-122.39724, 37.74977), -1, 0)
select st_asText(the_geom) from taxi.edges_vertices_pgr where id = 9957;

select * from pgr_dijkstra('SELECT id, source::int, target::int, len as cost, rlen as reverse_cost FROM taxi.edges', 9957, 9959, true, true);

select * from taxi.edges where st_distance_sphere(the_geom, st_point(-122.39724, 37.74977))<1000

select format('select id, source::int4, target::int4,len as cost, rlen as reverse_cost from taxi.edges	where st_distance_sphere(the_geom, st_point(%s, %s)) < 10000', to_char(st_x(st_point(-122.39724, 37.74977)),'999D999999999'), 
								to_char(st_y(st_point(-122.39724, 37.74977)),'999D999999999'))
;
select to_char(st_x(st_point(-122.39724, 37.74977)),'999D999999999')；

select taxi.get_trip_vertices(198104, true);
select taxi.get_trip_vertices(198104, false);

select st_asText(st_envelope('LineString(-122.528 37.8174, -122.346 37.7058)'::geometry));

select st_asText(st_envelope('LineString(-122.528 37.8174, -122.346 37.7058)'::geometry));
select st_asText('LineString(-122.528 37.7058, -122.346 37.8174)'::geometry)

select st_asText(st_envelope('LineString(0 0, 1 0)'::geometry));

select * from (
select row_number() over() id, x/1000000.0, y/1000000.0
from
	generate_series((-122.528*1000000)::int4, (-122.346*1000000)::int4, 4000) as x ,
	generate_series((37.7058*1000000)::int4, (37.8174*1000000)::int4, 4000) as y) as t
limit 100;

select st_distance_sphere(st_point(-122.528, 37.7058), st_point(-122.530, 37.7058))

select taxi.generate_grids(-122.528, 37.7058, -122.346, 37.8174, 0.002);
select count(*) from taxi.grids limit 10;

select taxi.generate_trips_od_grid();
select st_asText(st_envelope('LineString(-122.528 37.7058, -122.532 37.7098)'::geometry));

insert into taxi.trips_od_test select * from taxi.trips_od limit 1000
select count(*) from taxi.trips_od_grid limit 100;
select 
	o_grid, d_grid, count(*)
from
	taxi.trips_od_grid
group by
	o_grid, d_grid
order by 3 desc
limit 100;

select taxi.get_grid(-122.21147, 37.71421);
select st_asText(taxi.get_grid(1510))