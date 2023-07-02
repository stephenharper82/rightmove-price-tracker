import pathlib
import re
import logging as log
from yagmail import oauth2
import yagmail
import arrow
from jinja2 import Template
import os
import utils
from dotenv import load_dotenv
import tempfile

load_dotenv()

APP_GMAIL_ACCOUNT = os.getenv('APP_GMAIL_ACCOUNT')
GOOGLE_CLIENT_ID = os.getenv('GOOGLE_CLIENT_ID')
GOOGLE_CLIENT_SECRET = os.getenv('GOOGLE_CLIENT_SECRET')
GOOGLE_REFRESH_TOKEN = os.getenv('GOOGLE_REFRESH_TOKEN')
EMAIL_RECEIVER = os.getenv('EMAIL_RECEIVER')


def _configure_client_secret():
    tf = tempfile.NamedTemporaryFile(prefix='rightmove-tracker-email-oauth', suffix='.json', delete=False)
    fcontentsjson = f'''"google_client_id":"{GOOGLE_CLIENT_ID}",
"google_client_secret":"{GOOGLE_CLIENT_SECRET}", 
"google_refresh_token":  "{GOOGLE_REFRESH_TOKEN}"'''
    fcontentsjson = "{" + fcontentsjson + "}"
    with open(tf.name, 'w') as f:
        f.write(fcontentsjson)
    print(tf.name)
    return tf.name


client_secret_file = _configure_client_secret()

yag = yagmail.SMTP(user=APP_GMAIL_ACCOUNT, oauth2_file=client_secret_file)


def get_date(value):
    match = re.search(r'\d{4}-\d{2}-\d{2}', value)
    return match.group()


def convert_audit_history(ah: list):
    if ah:
        ahtml = "<ul>"
        for entry in ah:
            result = []
            for key, value in entry.items():
                if key != 'on':
                    result.append(f"{key}: {value['old']} vs {value['new']}")
            d = ",".join(result)
            ahtml += f"<li>{get_date(entry['on'])}: {d}</li>"
        ahtml += "</ul>"
        return ahtml
    else:
        return ""


email_template = Template("""
{% if var is iterable and (var is not string and var is not mapping) %}
<table>
<tr>
  <th>Property</th>
  <th>Address</th>
  <th>Price</th>
  <th>Added Or Reduced On</th>
  <th>Bedrooms</th>
  <th>Bathrooms</th>
  <th>Sub Type</th>
  <th>Status</th>
  <th>Diffs</th>
</tr>
{% for row in rows %}
<tr>
  <td><img src={{ row.image|e }} width=250></img></td>
  <td><a href=http://rightmove.co.uk{{ row.url }}>{{ row.address|e }}</a></td>
  <td>{{ "£{:,.2f}".format(row.price) }} ({{ row.price_type|e }})</td>
  <td>{{ row.added_or_reduced_on|e }}</td>
  <td>{{ row.bedrooms|e }}</td>
  <td>{{ row.bathrooms|e }}</td>
  <td>{{ row.propertySubType|e }}</td>
  <td>{{ row.status|e }}</td>
  <td>{{ convert_audit_history(row.audit_history) }}</td>
</tr>
{% endfor %}
</table>
{% else %}
NO DATA
{% endif %}

""")
email_template.globals['convert_audit_history'] = convert_audit_history


def translate(inserted_props, updated_props):
    try:
        html = f"""
        <h2>New Properties</h2>
        {email_template.render(rows=inserted_props)}
        <h2>Updated Properties</h2>
        {email_template.render(rows=updated_props)}
        """
        return html
    except TypeError as e:
        log.exception(
            f"Problem rendering email. Error: {e}\n\n Inserted props: {inserted_props} \n\n Updated props: {updated_props}")
        raise e


def send_email(inserted_props, updated_props):
    subject = f"Changes on {arrow.utcnow().date()} - {len(inserted_props)} new properties, {len(updated_props)} updated"
    html = translate(inserted_props, updated_props)
    utils.write_to_temp_file(contents=[html], file_prefix="rightmove_web_out", file_suffix=".txt")
    _send_email_internal(subject, html)


def _send_email_internal(subject, email_body):
    try:
        yag.send(
            to=EMAIL_RECEIVER,
            subject=subject,
            contents=[email_body],
        )
    except TypeError as e:
        log.exception(f"Problem sending email. Error: {e}\n\n Payload: {email_body}")
        raise e


def refresh_google_refresh_token():
    """Need to do this periodically until we move to PROD. Call this function and follow on-screen instructions"""
    print(oauth2.get_authorization(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET))


if __name__ == '__main__':
    # get_google_refresh_token()
    _send_email_internal('test emailer', 'test')
