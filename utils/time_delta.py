from datetime import datetime
import sys 

if len(sys.argv) != 3:
  print ('Usage: time_delta.py <from-time> <to-time>\n')
  exit(-1)
format_string='%a %b %d %H:%M:%S %Z %Y'
print('%d seconds' % (datetime.strptime(sys.argv[2], format_string) 
        - datetime.strptime(sys.argv[1], format_string)).seconds)
