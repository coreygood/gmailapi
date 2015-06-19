import sys

from gmailapi import GmailApi
def main():
    gmail = GmailApi()
    gmail.get_hourly()
    gmail.get_daily()
    gmail.get_newsletter_hourly()
    gmail.get_newsletter_daily()

# from gmapi import GmailApi
# def main():
#     gmail = GmailApi()
#     if len(sys.argv) > 1:
#         # gmail.get_hourly(sys.argv[1])
#         gmail.get_daily(sys.argv[1])
#         # gmail.get_newsletter_hourly(sys.argv[1])
#         # gmail.get_newsletter_daily(sys.argv[1])
#     else:
#         # gmail.get_hourly(0)
#         gmail.get_daily(0)
#         # gmail.get_newsletter_hourly(0)
#         # gmail.get_newsletter_daily(0)


if __name__ == "__main__":
    main()