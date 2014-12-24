use taxi;
drop procedure if exists get_node;
delimiter //
create procedure get_node(
	in v_id bigint
)language sql
begin
	select
		longitude, latitude
	from
		nodes
	where
		node_id = v_id
	;
	select
		k, v
	from
		node_tags
	where
		node_id = v_id
	;
end//
delimiter ;
