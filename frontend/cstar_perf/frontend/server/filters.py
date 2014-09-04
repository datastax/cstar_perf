import datetime as dt

def val_ago(value, unit="unit"):
    if value == 1:
        return "{} {} ago".format(value, unit)
    else:
        return "{} {}s ago".format(value, unit)

def human_date(dateval, nowfunc=dt.datetime.now):
    now = nowfunc()
    delta = now - dateval
    days = delta.days
    if days == 0:
        seconds = delta.seconds
        minutes = seconds / 60
        hours = minutes / 60
        if hours > 0:
            return val_ago(hours, unit="hour")
        if minutes > 0:
            return val_ago(minutes, unit="minute")
        return val_ago(seconds, unit="second")
    elif 0 < days < 7:
        return val_ago(days, unit="day")
    else:
        return dateval.strftime("%m/%d/%Y")

if __name__ == "__main__":
    jan1 = dt.datetime(2013, 1, 1)
    def test_case(expected, **kwargs):
        val = jan1 - dt.timedelta(**kwargs)
        human = human_date(val, nowfunc=lambda: jan1)
        assert human == expected, human
    test_case("1 day ago", days=1)
    test_case("2 days ago", days=2)
    test_case("5 seconds ago", seconds=5)
    test_case("2 minutes ago", seconds=60*2)
    test_case("3 hours ago", seconds=60*60*3)
    test_case("12/25/2012", days=7)
