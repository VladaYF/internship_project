from email.message import EmailMessage
import ssl
import smtplib
email_password = 'xonrofnxaohndoxw'
email_sender = 'fexample898@gmail.com'
email_reciver = 'isaeva.wlada@yandex.ru'

subject = "Vlada's Check emails"
body ="""
Hello!

I'm cheling new func
"""

em = EmailMessage()

em['From'] = email_sender
em['To'] = email_reciver
em['subject'] = subject 

em.set_content(body)

context_em = ssl.create_default_context()

with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context_em) as smtp:
   smtp.login(email_sender, email_password)
   smtp.sendmail(email_sender, email_reciver, em.as_string())

