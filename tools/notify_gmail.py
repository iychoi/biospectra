#! /usr/bin/env python

import os
import os.path
import sys
import smtplib

def login(username, password):
    session = smtplib.SMTP('smtp.gmail.com', 587)
    session.ehlo()
    session.starttls()
    session.login(username, password)
    return session

def sendmail(session, username, recipient, subject, body):
    headers = "\r\n".join(["from: " + username,
                           "subject: " + subject,
                           "to: " + recipient,
                           "mime-version: 1.0",
                           "content-type: text/html"])

    # body_of_email can be plaintext or html!           
    content = headers + "\r\n\r\n" + body
    session.sendmail(username, recipient, content)

def main():
    username = "bugsoda@gmail.com"
    password = ""
    recipient = "iychoi@email.arizona.edu"
    subject = "notify_gmail test"
    body = "notify_gmail test body!!!"
    

    session = login(username, password)
    sendmail(session, username, recipient, subject, body)
    session.close()
    print "message sent to", recipient

if __name__ == "__main__":
    main()

