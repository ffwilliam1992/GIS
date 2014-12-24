package com.wonder.util;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.RelationMember;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.RunnableSource;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.xml.common.CompressionMethod;
import org.openstreetmap.osmosis.xml.v0_6.XmlReader;

import com.wonder.db.DBUtil;

public class XML2MySQLImporter implements Sink {
	protected Set<String> wayType;

	private transient Connection connection;
	PreparedStatement insert_into_nodes_stmt;
	PreparedStatement insert_into_node_tags_stmt;
	PreparedStatement insert_into_ways_stmt;
	PreparedStatement insert_into_way_nodes_stmt;
	PreparedStatement insert_into_way_tags_stmt;
	PreparedStatement insert_into_relations_stmt;
	PreparedStatement insert_into_relation_members_stmt;
	PreparedStatement insert_into_relation_tags_stmt;

	private String file_name;

	public XML2MySQLImporter() {

	}

	public XML2MySQLImporter(String file_name) {
		this.file_name = file_name;
	}

	public void run()
	{
		File file = new File(file_name);
		RunnableSource reader = new XmlReader(file, true,
				CompressionMethod.None);
		reader.setSink(this);
		reader.run();
	}

	public String getFile_name()
	{
		return file_name;
	}

	public void setFile_name(String file_name)
	{
		this.file_name = file_name;
	}

	@Override
	public void process(EntityContainer entityContainer)
	{
		Entity entity = entityContainer.getEntity();
		switch (entity.getType()) {
		case Node:
			nodeHandler((Node) entity);
			break;
		case Bound:
			break;
		case Relation:
			relationHandler((Relation) entity);
			break;
		case Way:
			wayHandler((Way) entity);
			break;
		default:
			break;
		}
	}

	@Override
	public void release()
	{
		DBUtil.closeStatementResource(insert_into_nodes_stmt);
		DBUtil.closeStatementResource(insert_into_node_tags_stmt);
		DBUtil.closeStatementResource(insert_into_ways_stmt);
		DBUtil.closeStatementResource(insert_into_way_nodes_stmt);
		DBUtil.closeStatementResource(insert_into_way_tags_stmt);
		DBUtil.closeStatementResource(insert_into_relations_stmt);
		DBUtil.closeStatementResource(insert_into_relation_members_stmt);
		DBUtil.closeStatementResource(insert_into_relation_tags_stmt);
	}

	@Override
	public void complete()
	{
	}

	@Override
	public void initialize(Map<String, Object> arg0)
	{
		insert_into_nodes_stmt = DBUtil
				.createSqlStatement("insert into taxi.nodes "
						+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?)");
		try {
			connection = insert_into_nodes_stmt.getConnection();
			insert_into_node_tags_stmt = connection
					.prepareStatement("insert into taxi.node_tags "
							+ "values(?, ?, ?, ?)");
			insert_into_ways_stmt = connection
					.prepareStatement("insert into taxi.ways "
							+ "values(?, ?, ?, ?, ?, ?)");
			insert_into_way_nodes_stmt = connection
					.prepareStatement("insert into taxi.way_nodes "
							+ "values(?, ?, ?, ?)");
			insert_into_way_tags_stmt = connection
					.prepareStatement("insert into taxi.way_tags "
							+ "values(?, ?, ?, ?)");
			insert_into_relations_stmt = connection
					.prepareStatement("insert into taxi.relations "
							+ "values(?, ?, ?, ?, ?, ?)");
			insert_into_relation_members_stmt = connection
					.prepareStatement("insert into taxi.relation_members "
							+ "values(?, ?, ?, ?, ?, ?)");
			insert_into_relation_tags_stmt = connection
					.prepareStatement("insert into taxi.relation_tags "
							+ "values(?, ?, ?, ?)");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void nodeHandler(Node node)
	{
		try {
			// process nodes
			insert_into_nodes_stmt.setLong(1, node.getId());
			insert_into_nodes_stmt.setInt(2,
					(int) (node.getLatitude() * 10000000));
			insert_into_nodes_stmt.setInt(3,
					(int) (node.getLongitude() * 10000000));
			insert_into_nodes_stmt.setLong(4, node.getChangesetId());
			insert_into_nodes_stmt.setBoolean(5, true);
			insert_into_nodes_stmt.setTimestamp(6, new Timestamp(node
					.getTimestamp().getTime()));
			insert_into_nodes_stmt.setLong(7, 0);
			insert_into_nodes_stmt.setLong(8, node.getVersion());
			insert_into_nodes_stmt.setInt(9, 0);
			insert_into_nodes_stmt.execute();
			// process node tags
			insert_into_node_tags_stmt.setLong(1, node.getId());
			insert_into_node_tags_stmt.setLong(2, node.getVersion());
			for (Tag tag : node.getTags()) {
				insert_into_node_tags_stmt.setString(3, tag.getKey());
				insert_into_node_tags_stmt.setString(4, tag.getValue());
				insert_into_node_tags_stmt.execute();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("node\t" + node.getId());
	}

	public void wayHandler(Way way)
	{
		try {
			// process ways
			insert_into_ways_stmt.setLong(1, way.getId());
			insert_into_ways_stmt.setLong(2, way.getChangesetId());
			insert_into_ways_stmt.setTimestamp(3, new Timestamp(way
					.getTimestamp().getTime()));
			insert_into_ways_stmt.setBoolean(4, true);
			insert_into_ways_stmt.setLong(5, way.getVersion());
			insert_into_ways_stmt.setInt(6, 0);
			insert_into_ways_stmt.execute();
			// process way nodes
			insert_into_way_nodes_stmt.setLong(1, way.getId());
			insert_into_way_nodes_stmt.setLong(3, way.getVersion());
			long seq = 1;
			for (WayNode node : way.getWayNodes()) {
				insert_into_way_nodes_stmt.setLong(2, node.getNodeId());
				insert_into_way_nodes_stmt.setLong(4, seq++);
				insert_into_way_nodes_stmt.execute();
			}
			// process way tags
			insert_into_way_tags_stmt.setLong(1, way.getId());
			insert_into_way_tags_stmt.setLong(4, way.getVersion());
			for (Tag tag : way.getTags()) {
				insert_into_way_tags_stmt.setString(2, tag.getKey());
				insert_into_way_tags_stmt.setString(3, tag.getValue());
				insert_into_way_tags_stmt.execute();
			}
			System.out.println("way\t" + way.getId());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void relationHandler(Relation relation)
	{
		try {
			// process relations
			insert_into_relations_stmt.setLong(1, relation.getId());
			insert_into_relations_stmt.setLong(2, relation.getChangesetId());
			insert_into_relations_stmt.setTimestamp(3, new Timestamp(relation
					.getTimestamp().getTime()));
			insert_into_relations_stmt.setBoolean(4, true);
			insert_into_relations_stmt.setLong(5, relation.getVersion());
			insert_into_relations_stmt.setInt(6, 0);
			insert_into_relations_stmt.execute();
			// process relation members
			insert_into_relation_members_stmt.setLong(1, relation.getId());
			insert_into_relation_members_stmt.setLong(5, relation.getVersion());
			int seq = 1;
			for (RelationMember member : relation.getMembers()) {
				insert_into_relation_members_stmt.setString(2, member
						.getMemberType().name());
				insert_into_relation_members_stmt.setLong(3,
						member.getMemberId());
				insert_into_relation_members_stmt.setString(4,
						member.getMemberRole());
				insert_into_relation_members_stmt.setInt(6, seq++);
				insert_into_relation_members_stmt.execute();
			}
			// process relation tags
			insert_into_relation_tags_stmt.setLong(1, relation.getId());
			insert_into_relation_tags_stmt.setLong(4, relation.getVersion());
			for (Tag tag : relation.getTags()) {
				insert_into_relation_tags_stmt.setString(2, tag.getKey());
				insert_into_relation_tags_stmt.setString(3, tag.getValue());
				insert_into_relation_tags_stmt.execute();
			}
			System.out.println("relation\t" + relation.getId());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public Set<String> getWayType()
	{
		return wayType;
	}

	public void setWayType(HashSet<String> wayType)
	{
		this.wayType = wayType;
	}

}
