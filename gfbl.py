from gmailapi import GmailApi
import sys

def main():
    gmail = GmailApi()
    if len(sys.argv) > 1:
        gmail.get_daily_fbl(sys.argv[1])
    else:
        gmail.get_daily_fbl()
        
if __name__ == "__main__":
    main()