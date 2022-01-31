#This generator generates dates of death basing on the dates of birth with the option to set lifespan that is based on the weight list.
#It has been made by request of our user from forum http://forums.devart.com/viewtopic.php?f=23&t=33396   

import random
from System import *
from System import DateTime
from System import DBNull
from System.Globalization import CultureInfo

def main(): 

# DOB – name of the column in the current table, that contains dates of birth. 
# This name should be replaced with the actual name of the column of your table, for example DAYOFBIRTHDAY.
    
  #bd = DOB
  bd = DateofBirth
  if str(bd) == '':
   return DBNull.Value
  
# "dd/MM/yyyy" - is the format of date of birth. If it differs, make the following modifications.
  bdFormat = "dd/MM/yyyy" 
 
  x = random.randint(1, 100)

  deltaDays  = 0 

# Setting of percentage ranges for ages. The sum of all ranges should be equal to 100%.
  
  if x <= 5: # 5 %
      deltaDays = random.randint(1, 3650) # 5 % -  death at age 1..3650 days (1-10 years) 
  elif x > 5 and x <= 20: # 15 %
      deltaDays =  random.randint(3650, 18250) # 15 % - death at age 3650..18250 days (10-50 years) 
  else: # 80 %
      deltaDays =  random.randint(18250, 36500) # 80 % - death at age 18250..36500 days (50-100 years) 

  var_birthdate = ''

# Check of column type for the date of birth column (if it is a string column, the conversion to the DATE format should be performed).
  if type(bd).__name__ == 'str':
     var_birthdate = DateTime.ParseExact(bd,MM/dd/yyyy, CultureInfo.InvariantCulture)
  else: # else it's supposed to be DateTime
     var_birthdate = bd

# Addition of lifespan (in days).
  var_deathDate = var_birthdate.AddDays(deltaDays);

# If the date of death is bigger than the current date, the random quantity of days should be subtracted from the current date.
  
  if var_deathDate >= DateTime.Now:
   var_deathDate = DateTime.Now.AddDays(-random.randint(1, 300));

  if var_deathDate < var_birthdate:
    var_deathDate = var_birthdate

  return var_deathDate.Date 
    