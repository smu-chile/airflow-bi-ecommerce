
def delta_yearweeks(base_year, comp_year, years_53):
    print(base_year)
    print(comp_year)
    delta = (comp_year - base_year)*52
    print("OK")
    if base_year > comp_year:
        start_year = comp_year
        end_year = base_year
    else:
        start_year = base_year
        end_year = comp_year
    for year in range(start_year, end_year):
        print(f"year: {year}")
        if year in years_53:
            print("YES")
            delta = delta + 1 if delta > 0 else delta - 1 
    return delta