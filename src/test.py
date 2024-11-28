import os
import datetime as dt

d = dt.datetime.now()

filename = f'file {d}.txt'

# Write the formatted string to the new file
with open(filename, 'w') as file:
    file.write(f'test ran at {str(d)}')