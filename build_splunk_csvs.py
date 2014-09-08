#!/usr/bin/python

import httplib2

from apiclient.discovery import build
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run
from datetime import date, timedelta
import base64
import email
import csv
import operator
import re
from HTMLParser import HTMLParser

class GmailApi():
    # Path to the client_secret.json file downloaded from the Developer Console
    CLIENT_SECRET_FILE = '/Users/coreygood/Documents/GoodData/client_secret.json'

    # Check https://developers.google.com/gmail/api/auth/scopes for all available scopes
    OAUTH_SCOPE = 'https://www.googleapis.com/auth/gmail.readonly'

    # Location of the credentials storage file
    STORAGE = Storage('gmail.storage')

    # all hourly query label and subject combos to find the specific email reports; stored in dict for easy access
    HOURLY_QUERIES = dict([
        ('a1', 'label:_gooddata subject:A1 Hourly'),
        ('a2', 'label:_gooddata subject:A2 Hourly'),
        ('b1', 'label:_gooddata subject:B1 CM'),
        ('b2', 'label:_gooddata subject:B2 CT'),
        ('b3', 'label:_gooddata subject:B3 EL'),
        ('b4', 'label:_gooddata subject:B4 VR'),
        ('c1', 'label:_gooddata subject:C1 INV'),
        ('c2', 'label:_gooddata subject:C2 SG'),
        ('c3', 'label:_gooddata subject:C3 SPMHS'),
        ('c4', 'label:_gooddata subject:C4 SURBL'),
        ('d', 'label:_gooddata subject:D URI1+'),
        ('e', 'label:_gooddata subject:E Spam-by-Blacklist'),
        ('f', 'label:_gooddata subject:F URI-Proc'),
        ('g', 'label:_gooddata subject:G URI2+')
    ])

    # all daily query label and subject combos to find the specific email reports; stored in dict for easy access
    DAILY_QUERIES = dict([
        ('a1', 'label:_gooddata subject:A1 Daily'),
        ('a2', 'label:_gooddata subject:A2 Daily')
    ])

    FBL_QUERY = "label:_google-fbl"

    def __init__(self):
        # Start the OAuth flow to retrieve credentials
        self.flow = flow_from_clientsecrets(self.CLIENT_SECRET_FILE, scope=self.OAUTH_SCOPE)
        self.http = httplib2.Http()

        # Try to retrieve credentials from storage or run the flow to generate them
        self.credentials = self.STORAGE.get()
        if self.credentials is None or self.credentials.invalid:
          self.credentials = run(flow, self.STORAGE, http=self.http)

        # Authorize the httplib2.Http object with our credentials
        self.http = self.credentials.authorize(self.http)

        # Build the Gmail service from discovery
        self.gmail_service = build('gmail', 'v1', http=self.http)

    def get_daily(self):
        print "Gathering daily query data"
        self._get_daily_counts(self.DAILY_QUERIES, self._get_after_query())

    def get_hourly(self):
        print "Gathering hourly query data"
        self._get_hourly_counts(self.HOURLY_QUERIES, self._get_before_query(), self._get_after_query())

    def get_fbl(self):
        print "Gathering FBL data"
        self._get_fbl_data("{0}{1}".format(self.FBL_QUERY, self._fbl_after_query()))

    def _get_daily_counts(self, queries, after):
        full_dict = dict()
        full_dict = self._a_query(full_dict, queries['a2'], after, 'a2')
        full_dict = self._a_query(full_dict, queries['a1'], after, 'a1')
        self._save_daily_csv(full_dict)

    def _save_daily_csv(self, full_dict):
        filename = '/Users/coreygood/Documents/GoodData/InternalSpamDaily.csv'
        csvlist = []
        with open(filename, 'w') as csvwritefile:
            csvwriter = csv.writer(csvwritefile)
            date = self._get_yesterday()
            for uid in full_dict:
                csvwriter.writerow([uid, full_dict[uid]['a1'], full_dict[uid]['a2'], date])
        with open(filename, 'r') as csvreadfile:
            csvreader = csv.reader(csvreadfile, delimiter=',')
            for row in csvreader:
                csvlist.append(row)
        with open(filename, 'w') as csvsorted:
            csvsortwriter = csv.writer(csvsorted)
            sortlist = sorted(csvlist, key=lambda x: int(x[0]))
            csvsortwriter.writerow(["userid", "dc(msgid)", "total_volume", "date"])
            csvsortwriter.writerows(sortlist)

    def _a_query(self, full_dict, query, after, query_key):
        message_list = []            
        full_query = "{0}{1}".format(query, after)
        response = self.gmail_service.users().messages().list(userId='me', q=full_query).execute()
        if 'messages' in response:
            message_list.extend(response['messages'])
            for message in message_list:
                content = self.gmail_service.users().messages().get(userId='me', id=message['id'], format='full').execute()
                for part in content['payload']['parts']:
                    if part['mimeType'] == 'text/csv':
                        attachmentId = part['body']['attachmentId']
                content = self.gmail_service.users().messages().attachments().get(userId='me', messageId=message['id'], id=attachmentId).execute()
                file_data = base64.urlsafe_b64decode(content['data'].encode('UTF-8'))
                split_data = file_data.split('\r\n')
                full_dict = self._a_line_split(full_dict, split_data, query_key)
        return full_dict

    def _a_line_split(self, full_dict, split_data, query_key):
        i = 1
        last = len(split_data)
        for line in split_data:
            if i == 1 or i == last:
                i += 1
                continue
            split = line.split(',')
            if query_key == 'a2':
                full_dict = self._a2_add_dict(full_dict, split, query_key)
            else:
                full_dict = self._a1_add_dict(full_dict, split, query_key)
            i += 1
        return full_dict

    def _a2_add_dict(self, full_dict, split, query_key):
        count_dict = dict()
        count_dict['a2'] = split[1]
        count_dict['a1'] = 0
        full_dict[split[0]] = count_dict
        return full_dict

    def _a1_add_dict(self, full_dict, split, query_key):
        if split[0] in full_dict:
            count_dict = full_dict[split[0]]
        else:
            count_dict = dict()
            count_dict['a2'] = 0
        count_dict['a1'] = split[1]
        full_dict[split[0]] = count_dict
        return full_dict

    def _get_hourly_counts(self, queries, before, after):
        full_dict = dict()
        for query in queries:
            message_list = []
            count = 24
            count_dict = dict()
            full_query = "{0}{1}{2}".format(queries[query], before, after)
            response = self.gmail_service.users().messages().list(userId='me', q=full_query).execute()
            if 'messages' in response:
                message_list.extend(response['messages'])
                for message in message_list:
                    content = self.gmail_service.users().messages().get(userId='me', id=message['id'], format='raw').execute()
                    if query == 'e':
                        count_dict[count] = self._e_query_count(content)
                    else:
                        count_dict[count] = self._other_query_count(content)
                    count -= 1
            full_dict[query] = count_dict
        self._save_hourly_csv(full_dict)

    def _save_hourly_csv(self, full_dict):
        filename = '/Users/coreygood/Documents/GoodData/InternalSpamHourly.csv'
        csvlist = []
        with open(filename, 'w') as csvwritefile:
            csvwriter = csv.writer(csvwritefile)
            header_list = [
                'date', 'hour_timestamp', 'one_mailchannels_content_filter', 'total_mailchannels_verdicts', 
                'cloudmark_spam_verdicts', 'commtouch_spam_verdicts', 'eleven_spam_verdicts', 
                'vaderetro_spam_verdicts', 'ivm_spam_verdicts', 'sendgrid_spam_verdicts', 
                'spamhaus_spam_verdicts', 'surbl_spam_verdicts', 'one_or_more_uriblacklist_hit', 
                'ivm_unique_urls', 'sendgrid_unique_urls', 'spamhaus_unique_urls', 
                'surbl_unique_urls', 'total_processed_messages', 'two_or_more_uriblacklist_hits'
            ]
            csvwriter.writerow(header_list)
            date = self._get_yesterday()
            hour = 1
            while hour < 25:
                hour_list = [date, hour]
                query_list = ['a1', 'a2', 'b1', 'b2', 'b3', 'b4', 'c1', 'c2', 'c3', 'c4', 'd', 'e', 'f', 'g']
                bl_list = ["ivm", "sendgrid", "spamhaus", "surbl"]
                for query in query_list:
                    if query == 'e':
                        for bl in bl_list:
                            hour_list.append(full_dict[query][hour][bl])
                    else:
                        hour_list.append(full_dict[query][hour])
                hour += 1
                csvwriter.writerow(hour_list)

    def _e_query_count(self, content):
        e_dict = dict()
        split_message = self._split_message(content)
        match = [entry for entry in split_message if (
            "ivm" in entry or "spamhaus" in entry or "surbl" in entry or "sendgrid" in entry
        ) and "splunk" not in entry]
        for m in match:
            split = m.split()
            e_dict[split[1]] = split[0]
        while len(e_dict) < 4:
            bl_list = ["sendgrid", "ivm", "spamhaus", "surbl"]
            for blacklist in bl_list:
                if blacklist not in e_dict:
                    e_dict[blacklist] = 0
                    bl_list.remove(blacklist)
                    break
        return e_dict

    def _other_query_count(self, content):
        split_message = self._split_message(content)
        match = [entry for entry in split_message if "count" in entry]
        count_index = split_message.index(match[0])
        count = split_message[count_index+2].strip()
        return count

    def _split_message(self, content):
        msg_str = base64.urlsafe_b64decode(str(content['raw'])).encode('ASCII')
        message = email.message_from_string(msg_str)

        for part in message.walk():
            if part.get_content_type() == 'text/plain':
                text_plain = base64.b64decode(part.get_payload().strip())

        split_message = text_plain.split('\n')
        return split_message

    def _get_fbl_data(self, query):
        message_list = []
        response = self.gmail_service.users().messages().list(userId='me', q=query).execute()
        if 'messages' in response:
            message_list.extend(response['messages'])
            for message in message_list:
                content = self.gmail_service.users().messages().get(userId='me', id=message['id'], format='full').execute()
                for part in content['payload']['parts']:
                    if part['mimeType'] == 'text/html':
                        text_html = base64.urlsafe_b64decode(part['body']['data'].encode('UTF-8'))
                        parser = MyParser()
                        parser.feed(text_html)
                        parser.output()
                        parser.save_csv()
        else:
            print "No Gmail FBL Message Found."

    def _get_after_query(self):
        return " after:{0}".format(self._get_yesterday())

    def _get_before_query(self):
        return " before:{0}".format(self._get_today())

    def _fbl_after_query(self):
        return " after:{0}".format(self._get_today())

    def _get_today(self):
        return (date.today())

    def _get_yesterday(self):
        return self._subtract_days(1)

    def _get_two_days_ago(self):
        return self._subtract_days(2)

    def _subtract_days(self, num_days):
        return (self._get_today() - timedelta(days=num_days)).strftime("%Y/%m/%d")

class MyParser(HTMLParser):

    headers = ['date', 'spam_rate', 'userid', 'identifier']
    entries = []
    dict = dict()
    index = 0
    date = (date.today() - timedelta(days=2)).strftime("%m-%d-%Y")

    def handle_data(self, data):
        if self.index < 3:
            self.index += 1
        elif self.index % 3 == 0:
            if data == self.date:
                self.dict[self.headers[0]] = data
                self.index += 1
            else:
                print data
                self.dict = self.entries.pop()
                self.dict[self.headers[3]] += ' {0}'.format(data)
                self.entries.append(self.dict)
                self.dict = dict()
        elif self.index % 3 == 1:
            self.dict[self.headers[1]] = data
            self.index += 1
        elif self.index % 3 == 2:
            if '+' in data:
                split = data.split('+')
                self.dict[self.headers[2]] = split[0]
                self.dict[self.headers[3]] = split[1]
            else:
                self.dict[self.headers[2]] = data
                self.dict[self.headers[3]] = ''
            self.index += 1
            self.entries.append(self.dict)
            self.dict = dict()

    def save_csv(self):
        filename = '/Users/coreygood/Documents/GoodData/googlefbl.csv'
        csvlist = []
        with open(filename, 'w') as csvwritefile:
            csvwriter = csv.writer(csvwritefile)
            csvwriter.writerow(self.headers)
            for entry in self.entries:
                csvwriter.writerow([entry[self.headers[0]], entry[self.headers[1]], entry[self.headers[2]], entry[self.headers[3]]])

def main():
    gmail = GmailApi()
    gmail.get_hourly()
    gmail.get_daily()
    gmail.get_fbl()

if __name__ == "__main__":
    main()