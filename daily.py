from gmailapi import GmailApi

def main():
    gmail = GmailApi()
    # gmail.get_hourly()
    # gmail.get_daily()
    gmail.get_newsletter_hourly()
    gmail.get_newsletter_daily()

if __name__ == "__main__":
    main()