"""
This script contains functions that:

- Create heart rate thresholds for the user
- Validate heart rate doesn't exceed threshold
- Send an email to the user email.

The thresholds are set based on the maximum heart rate for the user based on their age and gender.
Several formulas are used to ensure accuracy for different categories of users.
These are:
- Tanaka Formula (women over age 40 and men of all ages): 208 - (0.7 x age)
- Gulati Formula (women only): 206 - (0.88 x age)
"""
