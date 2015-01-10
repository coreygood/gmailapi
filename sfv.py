from gmailapi import GmailApi
import sys

def main():
    gmail = GmailApi()
    gmail.get_sfv_all_time()
        
if __name__ == "__main__":
    main()