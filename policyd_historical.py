from gmailapi import GmailApi

class PolicydHistorical(GmailApi):

    POLICYD_QUERY = 'subject: Abnormal Activity'

    def get_historical_policyd(self):
        print "Gathering Historical PolicyD data"
        self._get_historical_policyd_data(self.POLICYD_QUERY)

    def _get_historical_policyd_data(self, query):
        # cur_date = self._get_today()
        # cur_date = datetime.date(2014, 10, 02)
        after = ' after: 2014-10-09'
        self.all_entries = []
        full_query = "{0}{1}".format(query, after)
        self._get_policyd(full_query)
        # self._save_historical_fbl_csv(self.all_entries)

    def _get_policyd(self, query):
        print "query: {0}".format(query)
        message_list = []
        response = self.gmail_service.users().messages().list(userId='me', q=query).execute()
        if 'messages' in response:
            message_list.extend(response['messages'])
            for message in message_list:
                content = self.gmail_service.users().messages().get(userId='me', id=message['id'], format='full').execute()
                print content
                # for part in content['payload']['parts']:
                #     if part['mimeType'] == 'text/html':
                #         text_html = base64.urlsafe_b64decode(part['body']['data'].encode('UTF-8'))
                #         parser = MyParser()
                #         parser.initialize(current_date)
                #         # parser.clear_entries()
                #         # parser.set_date(current_date)
                #         parser.feed(text_html)
                #         entries = parser.get_entries()
                #         if entries:
                #             self.all_entries.append(entries)
                #         # return entries
        else:
            print "No PolicyD Messages Found"


def main():
    policyd = PolicydHistorical()
    policyd.get_historical_policyd()

if __name__ == "__main__":
    main()