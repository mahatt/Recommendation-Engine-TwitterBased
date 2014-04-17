package model;

public class WeightedItem<WeightType>
{
	public Long id = null;
	public String name = null;
	public WeightType weight = null;

	public WeightedItem(String name, WeightType weight)
	{
		this.name = name;
		this.weight = weight;
	}

	public WeightedItem(Long id, WeightType weight)
	{
		this.id = id;
		this.weight = weight;
	}
}

