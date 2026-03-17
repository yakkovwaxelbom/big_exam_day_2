from datetime import datetime


a = datetime(2026, 3, 15, 10, 56, 31, 26465)
b = datetime(2026, 3, 15, 11, 2, 4, 459992)


avg = (a.timestamp() + b.timestamp())/2

avg_time = datetime.fromtimestamp(avg)
print(avg_time)