
import dateutil.parser

from elastalert.enhancements import BaseEnhancement
from elastalert.enhancements import DropMatchException

'''
class HourRangeEnhancement2(BaseEnhancement):
   def process(self, match):
       timestamp = dateutil.parser.parse(match[self.rule['timestamp_field']]).time()
       time_start = dateutil.parser.parse(self.rule['start_time']).time()
       time_end = dateutil.parser.parse(self.rule['end_time']).time()
       if time_start < timestamp < time_end:
           raise DropMatchException()'''

class HourRangeChoose(BaseEnhancement):
   def process(self, match):
       timestamp = dateutil.parser.parse(match[self.rule['timestamp_field']]).time()
       time_start = dateutil.parser.parse(self.rule['start_time']).time()
       time_end = dateutil.parser.parse(self.rule['end_time']).time() 
       if(self.rule['drop_if'] == 'outside'):
           if timestamp < time_start or timestamp > time_end:
               raise DropMatchException()
       elif(self.rule['drop_if'] == 'inside'):
           if timestamp >= time_start and timestamp <= time_end:
               raise DropMatchException()
