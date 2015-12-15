# Server email notifications to users:
import smtplib
from email.mime.text import MIMEText
import os
from jinja2 import Template, Environment, PackageLoader
import ConfigParser
import logging
import urlparse

from cstar_perf.frontend import SERVER_CONFIG_PATH
from cstar_perf.frontend.server.util import load_app_config

app_config = load_app_config()
template_env = Environment(loader=PackageLoader('cstar_perf.frontend.server', os.path.join('templates','email')))

log = logging.getLogger('cstar_perf.model')

class Email(object):
    message_template = 'default.jinja2' # name of jinja2 template file in templates/email
    subject = "Subject goes here - this is a regular jinja template - you can use {{vars}}"
    required_args = None # list of argument names that must appear in __init__ kwargs
    _vars = {
        "base_url": app_config.get('server', 'url')
    }
    additional_vars = {}

    def __init__(self, recipients=[], **kwargs):
        self.config = self.__get_config()
        self.setup(recipients, **kwargs)

    def __get_config(self, path=SERVER_CONFIG_PATH):
        defaults = {
            'from': 'cstar_perf@localhost',
            'server': 'localhost',
            'ssl': 'False',
            'port': '587'
        }
        config = ConfigParser.ConfigParser(allow_no_value=True)
        config.add_section('smtp')
        for k,v in defaults.items():
            config.set('smtp',k,v)
        config.read(path)
        cfg = dict(config.items('smtp'))
        cfg['ssl'] = config.getboolean('smtp','ssl')
        cfg['authenticate'] = cfg.has_key('user')
        return cfg

    def setup(self, recipients=[], **kwargs):
        self.recipients = recipients
        if type(recipients) in [str, unicode]:
            self.recipients = [recipients]
        if self.required_args is not None:
            missing_args = ", ".join(set(self.required_args).difference(kwargs.keys()))
            if len(missing_args) > 0:
                raise ValueError("Missing required args: {missing_args}".format(**locals()))
        if self.message_template == Email.message_template:
            raise NotImplementedError("You must subclass the Email class, defining your own message template")
        if self.subject == Email.subject:
            raise NotImplementedError("You must subclass the Email class, defining your own message subject")
        self.subject = Template(self.subject).render(**kwargs)

        v = kwargs.copy()
        v.update(self._vars)
        v.update(self.additional_vars)

        self.body = template_env.get_template(self.message_template).render(**v)

    def send(self):
        s = smtplib.SMTP(self.config['server'], self.config['port'])
        s.ehlo()
        if self.config['ssl']:
            s.starttls()
            s.ehlo()
        if self.config['authenticate']:
            s.login(self.config['user'], self.config['pass'])
        msg = MIMEText(self.body)
        msg['Subject'] = self.subject
        msg['From'] = self.config['from']
        msg['To'] = ", ".join(self.recipients)
        msg['BCC'] = self.config.get('always_bcc', None)
        try:
            s.sendmail(self.config['from'], self.recipients, msg.as_string())
        except smtplib.SMTPRecipientsRefused:
            log.warn("Bad recipients for mail (not an email address?) : {recipients}".format(recipients=self.recipients))
            return False
        log.info("Sent email to {recipients}".format(recipients=self.recipients))

class TestStatusUpdateEmail(Email):
    message_template = "test_status_update.jinja2"
    required_args = ('status','name','test_id')
    subject = "[cstar_perf] test {{status|upper}} - {{name}}"


class RegressionTestEmail(Email):
    message_template = "regression.jinja2"
    required_args = ('name','historical_performance','current_performance')
    subject = "[cstar_perf] Regression Detected in series {{name}}"
