package com.wonder.model;

import java.util.HashSet;
import java.util.Set;

public class Trip {
	private Set<Long> points;
	private long id;

	public Trip(long id) {
		this.id = id;
		points = new HashSet<Long>();
	}

	public Set<Long> getPoints()
	{
		return points;
	}

	public void setPoints(Set<Long> points)
	{
		this.points = points;
	}

	public long getId()
	{
		return id;
	}

	public void setId(long id)
	{
		this.id = id;
	}

	public boolean add(Long e)
	{
		return points.add(e);
	}

	public int size()
	{
		return points.size();
	}

	@Override
	public String toString()
	{
		return "\n" + id + "\t" + points.toString();
	}
}
