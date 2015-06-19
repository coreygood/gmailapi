#!/usr/bin/python

import httplib2

from apiclient.discovery import build
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run
import datetime
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

    FBL_QUERY = 'label:_google-fbl'

    NEWSLETTER_HOURLY_QUERIES = dict([
        ('spam', 'label:_gooddata subject:Newsletter Spam Hourly'),
        ('total', 'label:_gooddata subject:Newsletter Total Messages Hourly')
    ])

    NEWSLETTER_DAILY_QUERIES = dict([
        ('spam', 'label:_gooddata subject:Newsletter Spam Daily'),
        ('total', 'label:_gooddata subject:Newsletter Total Messages Daily')
    ])

    SFV_QUERY = 'label:sfv_strangeness to: support-info@sendgrid.com'

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

    def get_daily(self, days=0):
        print "Gathering daily query data"
        self._get_daily_counts(self.DAILY_QUERIES, self._get_before_query(days), self._get_after_query(days), days)

    def get_hourly(self, days=0):
        print "Gathering hourly query data"
        self._get_hourly_counts(self.HOURLY_QUERIES, self._get_before_query(), self._get_after_query(), False)

    def get_daily_fbl(self, days=0):
        print "Gathering FBL data"
        self._get_fbl_data("{0}{1}".format(self.FBL_QUERY, self._fbl_after_query(days)), days)

    def get_historical_fbl(self):
        print "Gathering Historical FBL data"
        self._get_historical_fbl_data(self.FBL_QUERY)

    def get_newsletter_hourly(self):
        print "Gathering Newsletter hourly data"
        self._get_hourly_counts(self.NEWSLETTER_HOURLY_QUERIES, self._get_before_query(), self._get_after_query(), True)

    def get_newsletter_daily(self):
        print "Gathering Newsletter daily data"
        self._get_newsletter_daily_counts(self.NEWSLETTER_DAILY_QUERIES, self._fbl_after_query(1))

    def get_sfv_all_time(self):
        print "Gathering SFV data"
        self._get_sfv_data(self.SFV_QUERY)

    def _get_sfv_data(self, query):
        message_list = []
        # count = 1
        response = self.gmail_service.users().messages().list(userId='me', q=query).execute()
        # print response
        if 'messages' in response:
            message_list.extend(response['messages'])
        for message in message_list:
            content = self.gmail_service.users().messages().get(userId='me', id=message['id'], format='full').execute()
            # print "{0}: {1}".format(count, content['payload']['headers'])
            # count += 1
            date = ""
            for part in content['payload']['headers']:
                if part['name'] == 'Subject':
                    print "{0};{1}".format(date, part['value'])
                    continue
                elif part['name'] == 'Date':
                    date = part['value']
                # if part['value'] == 'support-info@sendgrid.com':
                #     for p in content['payload']['headers']:
                #         if p['name'] == 'Subject':
                #             print p['value']
                #             continue
                # else:
                #     continue

    def _get_daily_counts(self, queries, before, after, days):
        full_dict = dict()
        full_dict = self._a_query(full_dict, queries['a2'], before, after, 'a2')
        full_dict = self._a_query(full_dict, queries['a1'], before, after, 'a1')
        self._save_daily_csv(days, full_dict)

    def _get_newsletter_daily_counts(self, queries, after):
        full_dict = dict()
        for key in queries.keys():
            full_dict = self._a_query(full_dict, queries[key], after, key)
        self._save_newsletter_daily_csv(full_dict)

    def _get_date_for_small_save(self, num_days):
        save_day = date.today() - timedelta(days=num_days)
        return "{0}/{1}/{2}".format(save_day.month, save_day.day, save_day.strftime('%y'))

    def _get_date_for_save(self, days):
        today = date.today() - timedelta(days=int(days))
        return "{0}/{1}/{2}".format(today.year, today.month, today.day)

    def _save_daily_csv(self, days, full_dict):
        today = self._get_date_for_save(days)
        filename = "/Users/coreygood/Documents/GoodData/archive/{0}/InternalSpamDaily.csv".format(today)
        csvlist = []
        with open(filename, 'wb') as csvwritefile:
            csvwriter = csv.writer(csvwritefile, lineterminator='\r')
            # yest = date.today() - timedelta(days=1)
            # day = "{0}/{1}/{2}".format(yest.month, yest.day, yest.strftime('%y'))
            # date = self._get_date_for_small_save(num_days=1)
            # date = self._get_yesterday()
            date = self._subtract_days(int(days)+1)
            csvwriter.writerow(["userid", "dc(msgid)", "total_volume", "date"])
            # print full_dict
            temp_dict = sorted(full_dict.items(), key=lambda x: int(x[0]))
            for uid in temp_dict:
                csvwriter.writerow([uid[0], uid[1]['a1'], uid[1]['a2'], date])
            # for uid in full_dict:
            #     csvwriter.writerow([uid, full_dict[uid]['a1'], full_dict[uid]['a2'], date])
        # with open(filename, 'r') as csvreadfile:
        #     csvreader = csv.reader(csvreadfile, delimiter=',')
        #     for row in csvreader:
        #         csvlist.append(row)
        # with open(filename, 'wb') as csvsorted:
        #     csvsortwriter = csv.writer(csvsorted, lineterminator='\r')
        #     sortlist = sorted(csvlist, key=lambda x: int(x[0]))
        #     csvsortwriter.writerow(["userid", "dc(msgid)", "total_volume", "date"])
        #     csvsortwriter.writerows(sortlist)

    def _save_newsletter_daily_csv(self, full_dict):
        today = self._get_date_for_save()
        filename = "/Users/coreygood/Documents/GoodData/archive/{0}/NLSpamDaily.csv".format(today)
        csvlist = []
        with open(filename, 'wb') as csvwritefile:
            csvwriter = csv.writer(csvwritefile, lineterminator='\r')
            csvwriter.writerow(["userid", "spam messages", "total messages", "date"])
            date = self._get_yesterday()
            # date = self._get_date_for_small_save(num_days=1)
            temp_dict = sorted(full_dict.items(), key=lambda x: int(x[0]))
            for uid in temp_dict:
                csvwriter.writerow([uid[0], uid[1]['spam'], uid[1]['total'], date])
        # with open(filename, 'r') as csvreadfile:
        #     csvreader = csv.reader(csvreadfile, delimiter=',')
        #     for row in csvreader:
        #         csvlist.append(row)
        # with open(filename, 'wb') as csvsorted:
        #     csvsortwriter = csv.writer(csvsorted, lineterminator='\r')
        #     sortlist = sorted(csvlist, key=lambda x: int(x[0]))
        #     csvsortwriter.writerow(["userid", "spam messages", "total messages", "date"])
        #     csvsortwriter.writerows(sortlist)

    def _a_query(self, full_dict, query, before, after, query_key):
        message_list = []            
        full_query = "{0}{1}{2}".format(query, before, after)
        print full_query
        response = self.gmail_service.users().messages().list(userId='me', q=full_query).execute()
        if 'messages' in response:
            message_list.extend(response['messages'])
            for message in message_list:
                content = self.gmail_service.users().messages().get(userId='me', id=message['id'], format='full').execute()
                print content
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
            elif query_key == 'total':
                full_dict = self._total_add_dict(full_dict, split, query_key)
            elif query_key == 'spam':
                full_dict = self._spam_add_dict(full_dict, split, query_key)
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


    def _total_add_dict(self, full_dict, split, query_key):
        count_dict = dict()
        count_dict['total'] = split[1]
        count_dict['spam'] = 0
        full_dict[split[0]] = count_dict
        return full_dict

    def _spam_add_dict(self, full_dict, split, query_key):
        if split[0] in full_dict:
            count_dict = full_dict[split[0]]
        else:
            count_dict = dict()
            count_dict['total'] = 0
        count_dict['spam'] = split[1]
        full_dict[split[0]] = count_dict
        return full_dict

    def _get_hourly_counts(self, queries, before, after, news_bool):
        full_dict = dict()
        for query in queries:
            message_list = []
            count = 24
            count_dict = dict()

            # in case an alert does not send, uncomment and fill in the following three values
            # missed_hours = [10] # hour(s) of alert that didn't send
            # missed_amount = 0 # total from the alert that didn't send
            # # # missed queries for internal hourly data
            # # missed_queries = ['a1','a2','b1','b2','b3','b4','c1','c2','c3','c4','d','e','f','g',]
            # # # missed queries for newsletter hourly data
            # missed_queries = ['spam', 'total'] # query from alert that didn't send; should be 'spam' or 'total'
            # # in case an alert does not send, uncomment
            
            full_query = "{0}{1}{2}".format(queries[query], before, after)
            response = self.gmail_service.users().messages().list(userId='me', q=full_query).execute()

            if 'messages' in response:
                message_list.extend(response['messages'])
                # # print "length {0}: {1}".format(len(message_list), message_list)
                # # Uncomment if an alert does not send
                # if query in missed_queries:
                #     for missed_hour in missed_hours:
                #         if query == 'e':
                #             count_dict[missed_hour] = {'ivm': 0, 'spamhaus': 0, 'sendgrid': 0, 'surbl': 0}
                #         else:
                #             count_dict[missed_hour] = missed_amount
                # # Uncomment if an alert does not send
                print "query: {0}, count: {1}".format(query, len(message_list))
                for message in message_list:
                    content = self.gmail_service.users().messages().get(userId='me', id=message['id'], format='raw').execute()
                    if query == 'e':
                        count_dict[count] = self._e_query_count(content)
                    else:
                        count_dict[count] = self._other_query_count(content)
                    count -= 1
                    # # Uncomment if an alert does not send
                    # if count in missed_hours and query in missed_queries:
                    #     count -= 1
                    # print count_dict
                    # # Uncomment if an alert does not send
            full_dict[query] = count_dict
            print full_dict[query]
        if news_bool:
            self._save_newsletter_hourly_csv(full_dict)
        else:
            self._save_hourly_csv(full_dict)

    def _save_hourly_csv(self, full_dict):
        today = self._get_date_for_save()
        filename = "/Users/coreygood/Documents/GoodData/archive/{0}/InternalSpamHourly.csv".format(today)
        csvlist = []
        with open(filename, 'wb') as csvwritefile:
            csvwriter = csv.writer(csvwritefile, lineterminator='\r')
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

    def _save_newsletter_hourly_csv(self, full_dict):
        today = self._get_date_for_save()
        filename = "/Users/coreygood/Documents/GoodData/archive/{0}/NLSpamHourly.csv".format(today)
        csvlist = []
        with open(filename, 'wb') as csvwritefile:
            csvwriter = csv.writer(csvwritefile, lineterminator='\r')
            header_list = [
                'date', 'hour_timestamp', 'spam', 'total'
            ]
            csvwriter.writerow(header_list)
            date = self._get_yesterday()
            hour = 1
            while hour < 25:
                hour_list = [date, hour]
                query_list = ['spam', 'total']
                for query in query_list:
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
        # print split_message
        match = [entry for entry in split_message if "count" in entry]
        if match:
            count_index = split_message.index(match[0])
            count = split_message[count_index+2].strip()
        else:
            count = 0
        # print count
        return count

    def _split_message(self, content):
        msg_str = base64.urlsafe_b64decode(str(content['raw'])).encode('ASCII')
        message = email.message_from_string(msg_str)

        for part in message.walk():
            if part.get_content_type() == 'text/plain':
                text_plain = base64.b64decode(part.get_payload().strip())

        split_message = text_plain.split('\n')
        return split_message

    def _get_fbl_data(self, query, days):
        message_list = []
        response = self.gmail_service.users().messages().list(userId='me', q=query).execute()
        print response
        if 'messages' in response:
            message_list.extend(response['messages'])
            # i = 0
            for message in message_list:
                content = self.gmail_service.users().messages().get(userId='me', id=message['id'], format='full').execute()
                # print content
                for part in content['payload']['parts']:
                    if part['mimeType'] == 'text/html':
                        text_html = base64.urlsafe_b64decode(part['body']['data'].encode('UTF-8'))
                        parser = MyParser()
                        parser.initialize(self._get_today())
                        parser.feed(text_html)
                        parser.save_csv(days)
                        # parser.save_csv(days, i)
                        # i += 1
        else:
            print "No Gmail FBL Message Found."

    def _get_historical_fbl_data(self, query):
        # cur_date = self._get_today()
        cur_date = datetime.date(2014, 10, 02)
        self.all_entries = []
        while self._strftime_fbl_data(cur_date) > '09-14-2014':
            full_query = "{0}{1}".format(query, self._fbl_historical_query(cur_date))
            print "On date: {0}".format(cur_date)
            # print full_query
            # cur_entries = self._get_fbl(full_query, cur_date)
            self._get_fbl(full_query, cur_date)
            # print cur_entries
            # if cur_entries:
            #     all_entries.append(cur_entries)
                # self._save_historical_fbl_csv(cur_entries, first)
            # print "\n\n"
            # decrement date
            cur_date = self._subtract_one_day_fbl(cur_date)
            # print all_entries
        # print self.all_entries
        self._save_historical_fbl_csv(self.all_entries)

    def _get_fbl(self, query, current_date):
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
                        parser.initialize(current_date)
                        # parser.clear_entries()
                        # parser.set_date(current_date)
                        parser.feed(text_html)
                        entries = parser.get_entries()
                        if entries:
                            self.all_entries.append(entries)
                        # return entries
        else:
            print "No Gmail FBL Message Found on {0}.".format(current_date)
            return None

    def _get_after_query(self, days):
        return " after:{0}".format(self._subtract_days(int(days)))
        # return " after:{0}".format(self._get_yesterday())

    def _get_before_query(self, days):
        return " before:{0}".format(self._subtract_days(int(days)-1))
        # return " before:{0}".format(self._get_today())

    def _fbl_after_query(self, days):
        return " after:{0}".format(self._subtract_days(int(days)))

    def _fbl_historical_query(self, current_date):
        before_date = self._subtract_days_from_current_date(current_date, -1)
        return " before:{0} after:{1}".format(self._strftime_gmail_search(before_date), self._strftime_gmail_search(current_date))

    def _strftime_gmail_search(self, date):
        return date.strftime('%Y-%m-%d')

    def _strftime_fbl_data(self, date):
        return date.strftime('%m-%d-%Y')

    def _get_today(self):
        return (date.today())

    def _get_yesterday(self):
        return self._subtract_days(1)

    def _get_two_days_ago(self):
        return self._subtract_days(2)

    def _subtract_days(self, num_days):
        return (self._get_today() - timedelta(days=num_days)).strftime('%Y/%m/%d')

    def _subtract_one_day_fbl(self, current_date):
        return self._subtract_days_from_current_date(current_date, 1)

    def _subtract_days_from_current_date(self, current_date, num_days):
        return (current_date - timedelta(days=num_days))#.strftime("%m-%d-%Y")

    def _save_historical_fbl_csv(self, all_entries):
        filename = '/Users/coreygood/Documents/GoodData/googlefbl.csv'
        headers = ['date', 'spam_rate', 'userid', 'campaign']
        with open(filename, 'wb') as csvwritefile:
            csvwriter = csv.writer(csvwritefile, lineterminator='\r')
            csvwriter.writerow(headers)
            for daily_entries in all_entries:
                for entry in daily_entries:
                    csvwriter.writerow([entry[headers[0]], entry[headers[1]], entry[headers[2]], entry[headers[3]]])

class MyParser(HTMLParser):

    def initialize(self, date):
        self.headers = ['date', 'spam_rate', 'userid', 'campaign']
        self.entries = []
        self.dict = dict()
        self.index = 0
        self.date = (date - timedelta(days=1)).strftime('%m-%d-%Y')

    def handle_data(self, data):
        # print data
        if self.index < 3:
            self.index += 1
        elif self.index % 3 == 0:
            # print "len:{0}, data:'{1}'".format(len(data), data)
            # print self.is_date(data)
            # if data == self.date:
            if self.is_date(data):
                self.dict[self.headers[0]] = data
                self.index += 1
            else:
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

    def get_entries(self):
        return self.entries

    def is_date(self, date):
        if date.count('-') == 2 and len(date) == 10 and date.find('-') == 2 and date.rfind('-') == 5:
            return True
        else:
            return False

    # def clear_entries(self):
    #     self.entries = []

    # def set_date(self, date):
    #     self.date = (date - timedelta(days=1)).strftime('%m-%d-%Y')
    def _get_date_for_save(self, days):
        today = date.today() - timedelta(days=days)
        return "{0}/{1}/{2}".format(today.year, today.month, today.day)

    # def save_csv(self, days, num):
    def save_csv(self, days):
        today = self._get_date_for_save(int(days))
        # filename = "/Users/coreygood/Documents/GoodData/archive/{0}/googlefbl{1}.csv".format(today, num)
        filename = "/Users/coreygood/Documents/GoodData/archive/{0}/googlefbl.csv".format(today)
        csvlist = []
        with open(filename, 'wb') as csvwritefile:
            csvwriter = csv.writer(csvwritefile, lineterminator='\r')
            csvwriter.writerow(self.headers)
            for entry in self.entries:
                csvwriter.writerow([entry[self.headers[0]], entry[self.headers[1]], entry[self.headers[2]], entry[self.headers[3]]])

