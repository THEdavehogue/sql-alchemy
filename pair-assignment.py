#!/usr/bin/env python
"""
## install related
sudo apt-get install python-psycopg2 python-sqlalchemy
sudo pip install sqlalchemy_schemadisplay

## db related
sudo su - postgres
psql -U postgres

CREATE USER ender WITH ENCRYPTED PASSWORD 'bugger';
CREATE DATABASE socialmedia WITH OWNER ender;
\q

cd /home/adam/repos/sql/data/
psql socialmedia < socialmedia.sql


## if you have permissions issues
sudo su - postgres
psql -U postgres
\c socialmedia
GRANT ALL PRIVILEGES ON TABLE optout logins  to ENDER;
GRANT ALL PRIVILEGES ON TABLE optout, logins, messages, registrations, test_group, friends TO ender;
"""

import sys,datetime
from dblib import DbWrapper
import numpy as np
import pandas as pd

## basic connect
db = DbWrapper('ender','bugger','socialmedia')

## explore the table
db.print_summary()
db.draw_schema('socialmedia.png')

all_registrations = db.session.query(db.tables['registrations']).all()
some_registrations = all_registrations[:10]

for reg in some_registrations:
    print(reg), reg.type

registrationTypes = np.array([reg.type for reg in all_registrations])
registrationTmstmp = np.array([reg.tmstmp for reg in all_registrations])
registrationUsers = np.array([reg.userid for reg in all_registrations])
sortedInds = np.argsort(registrationTmstmp)

df = pd.DataFrame(data={'regType':registrationTypes[sortedInds],
                        'regTmstmp':registrationTmstmp[sortedInds],
                        'regUsers':registrationUsers[sortedInds]})

print(df['regType'].value_counts())

##  1. Get the number of users who have registered each day, ordered by date
df['day'] = ["%s.%s.%s"%(tmstmp.year,tmstmp.month,tmstmp.day) for tmstmp in registrationTmstmp]
print("\n.......")
print(df['day'].value_counts())

## 2. Which day of the week gets the most registrations?
df['weekday'] = [tmstmp.strftime("%A") for tmstmp in df['regTmstmp']]
print("\n.......")
print(df['weekday'].value_counts())

## 3. You are sending an email to users who haven't logged in in the week before '2014-08-14'
##    and have not opted out of receiving email. Write a query to select these users.
end_date = datetime.datetime.strptime("08/14/14", "%m/%d/%y")
start_date = end_date - datetime.timedelta(days=7)

Registrations = db.tables['registrations']
Optout = db.tables['optout']
Logins = db.tables['logins']

query1 = db.session.query(Registrations.c.userid)
query2 = db.session.query(Logins.c.userid).\
        filter(Logins.c.tmstmp >= start_date,Logins.c.tmstmp < end_date)
query3 = db.session.query(Optout.c.userid)

q1 = set([q[0] for q in query1.all()])
q2 = set([q[0] for q in query2.all()])
q3 = set([q[0] for q in query3.all()])

users_to_mail = list(q1.difference(q2.union(q3)))
print("users to mail: %s"%users_to_mail)

## 4. For every user, get the number of users who registered on the same day as them.
##    Hint: This is a self join (join the registrations table with itself
df['sameday_registrations'] = [np.where(df['day'].values == df['day'][idx])[0].size - 1 for user,idx in enumerate(registrationUsers)]

## 5. You are running an A/B test and would like to target users who have logged in on mobile more
##    times than web. You should only target users in test group A. Write a query to get all the targeted users.

all_logins = db.session.query(Logins.c.userid,Logins.c.type).all()
loginUsers = np.array([log[0] for log in all_logins])
loginTypes = np.array([log[1] for log in all_logins])
webIndices = np.where(loginTypes=='web')[0]
mobileIndices = np.where(loginTypes=='mobile')[0]

print np.unique(loginTypes)
df['login-web-counts'] = [np.where(loginUsers[webIndices]==user)[0].size for user in registrationUsers]
df['login-mobile-counts'] = [np.where(loginUsers[mobileIndices]==user)[0].size for user in registrationUsers]
mask = (df['login-web-counts'] - df['login-mobile-counts']) > 0
groupA = df['regUsers'][mask].values
groupB = df['regUsers'][~mask].values
print groupA

## 6. You would like to determine each user's most communicated with user. For each user,
##    determine the user they exchange the most messages with (outgoing plus incoming).

Messages = db.tables['messages']
all_messages = db.session.query(Messages.c.sender,Messages.c.recipient).all()
msgSender = np.array([msg[0] for msg in all_messages])
msgRecipient = np.array([msg[1] for msg in all_messages])

user = 1
def get_most_comm_user(user):
    sentInds = np.where(msgSender==user)[0]
    recvInds = np.where(msgRecipient==user)[0]
    communicatedWith1 = msgRecipient[sentInds]
    communicatedWith2 = msgRecipient[recvInds]
    communicatedWith = np.hstack((communicatedWith1,communicatedWith2))
    com_users = np.sort(np.unique(communicatedWith))
    counts = np.array([np.where(communicatedWith==cuser)[0].size for cuser in com_users])
    sortedInds = np.argsort(counts)[::-1]
    if com_users[sortedInds[0]] != user:
        return(com_users[sortedInds[0]])
    else:
        return(com_users[sortedInds[1]])

df['most_comm_user'] = [get_most_comm_user(user) for user in registrationUsers] 

