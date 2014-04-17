package enumrators;

public enum UM_Type
{
	Entity_based,  Topic_based,  Hashtag_based,  Entity_type_based;

	public String toString()
	{
		if (Entity_based.equals(this)) {
			return "entity";
		}
		if (Entity_type_based.equals(this)) {
			return "entityType";
		}
		if (Topic_based.equals(this)) {
			return "topic";
		}
		if (Hashtag_based.equals(this)) {
			return "hashtag";
		}
		return "none";
	}
}

