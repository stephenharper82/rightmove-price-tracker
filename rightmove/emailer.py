import yagmail

# help from https://realpython.com/python-send-email/
email = 'rightmovepricetracker'
password ='Trackman99!'


receiver = "stephen.p.haprer@outlook.com"
body = "Hello there from Yagmail"
filename = "document.pdf"

yag = yagmail.SMTP(user=email, password=password)
yag.send(
    to=receiver,
    subject="Yagmail test with attachment",
    contents=body,
    #attachments=filename,
)


