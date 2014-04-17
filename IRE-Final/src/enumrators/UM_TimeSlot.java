package enumrators;

public enum UM_TimeSlot
{
	Weekdays,  Weekends,  MonTue,  WedThur,  Day,  Night,  DayEvenHours,  DayOddHours,  NightEvenHours,  NightOddHours,  All;

	public String toString()
	{
		if (Weekdays.equals(this)) {
			return "weekdays";
		}
		if (Weekends.equals(this)) {
			return "weekends";
		}
		if (MonTue.equals(this)) {
			return "Monday&Tuesday";
		}
		if (WedThur.equals(this)) {
			return "Wednesday&Thursday";
		}
		if (Day.equals(this)) {
			return "day[9am-17pm]";
		}
		if (Night.equals(this)) {
			return "night[18pm-3am]";
		}
		if (DayEvenHours.equals(this)) {
			return "DayEvenHours";
		}
		if (DayOddHours.equals(this)) {
			return "DayOddHours";
		}
		if (NightEvenHours.equals(this)) {
			return "NightEvenHours";
		}
		if (NightOddHours.equals(this)) {
			return "NightOddHours";
		}
		return "all";
	}
}
