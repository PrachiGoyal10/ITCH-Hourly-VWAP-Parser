# ITCH-Hourly-VWAP-Parser
This program reads NASDAQ ITCH 5.0 data, calculates VWAPs for each stock in every trading hour. While reading and parsing the data, the VWAPs are calculated hourly after trading starts.

-- Input: unzipped ITCH 5.0 daily file
-- Output: csv file containing hourly VWAPs for each stock


# Lengths of ITCH messages according to NASDAQ specifications
ITCH_FORMAT_LENGTH = {
    b'S': 11,  # System Event Message
    b'R': 38,  # Stock Directory Message
    b'H': 24,  # Stock Trading Action Message
    b'Y': 19,  # Reg SHO Short Sale Price Test Restricted Indicator
    b'L': 25,  # Market Participant Position Message
    b'V': 34,  # MWCB Decline Level Message
    b'W': 11,  # MWCB Status Message
    b'K': 27,  # IPO Quoting Period Update Message
    b'J': 35,  # Limit Up/Down Auction Collar Message
    b'h': 19,  # Operational Halt Message
    b'A': 35,  # Add Order No MPID Attribution Message
    b'F': 39,  # Add Order MPID Attribution Message
    b'E': 24,  # Order Executed Message
    b'C': 30,  # Order Executed With Price Message
    b'X': 19,  # Order Cancel Message
    b'D': 17,  # Order Delete Message
    b'U': 38,  # Order Replace Message
    b'P': 40,  # Non-Cross Trade Message
    b'Q': 39,  # Cross Trade Message
    b'B': 36,  # Broken Trade Execution Message
    b'I': 50,  # Net Order Imbalance Indicator (NOII) Message
}


The format is defined by the following document:

https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQ TVITCHspecification.pdf

