from datetime import date
from dateutil.relativedelta import relativedelta

# Returns the year-month string (yyyy-MM) for the given number of months ago.
def get_target_yyyymm(months_ago=3):

    target_date = date.today() - relativedelta(months=months_ago)
    return target_date.strftime("%Y-%m")


# Returns the date representing the first day of the month, 'n' months ago.
def get_month_start_n_months_ago(months_ago: int = 3) -> date:

    return date.today().replace(day=1) - relativedelta(months=months_ago)