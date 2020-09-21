import mailbox
import string
import re
from datetime import datetime as dt
#from html.parser import HTMLParser
from bs4 import BeautifulSoup
import pandas as pd

# Add a pandas_datareader-like data reader class that takes the same arges as
# the function and provides a .read() method to Asset.


# class StripHTML(HTMLParser):
#     def __init__(self):
#         super().__init__()
#         self.buf = ''
#         self.tabrow = False
#     def handle_data(self, data):
#         if self.tabrow:
#             data = data.replace('\n', '') + ' '
#         self.buf += data
#     def handle_starttag(self, tag, attrs):
#         if tag == 'tr':
#             self.tabrow = True
#     def handle_endtag(self, tag):
#         if tag == 'tr':
#             self.tabrow = False
#             self.buf += '\n'
#     def content(self):
#         return self.buf


def scrape_fx_mbox(path, from_c='GBP', to_c='CLP',
                   date_fmt='({DATE})[ \t]+({TIME})[ \t]+({TZ})',
                   row_fmt='({SYMBOL})[ \t]+({TO_BASE})[ \t]+({FROM_BASE})'):
    """
    Scrape tabular currency data from email in an mbox-format file.
    """

    date_sep = '[-/. ]+'
    date_re = '[0-9]{{4}}{sep}[0-9]{{1,2}}{sep}[0-9]{{1,2}}'.format(sep=date_sep)
    time_re = '[0-9]{{1,2}}{sep}[0-9]{{2}}{sep}[0-9]{{2}}[0-9.]*'.format(sep=':')
    tz_re = '[A-Z]{3}'
    dt_re = re.compile(date_fmt.format(DATE=date_re, TIME=time_re, TZ=tz_re))
    numeric_re = '[0-9,.]+'
    rates_re = re.compile(row_fmt.format(SYMBOL='[A-Z]{3}',
                          TO_BASE=numeric_re, FROM_BASE=numeric_re))
    tag_re = re.compile('<.*?>')

    pos = {key : n for n, (pos, key) in enumerate(sorted((
              (row_fmt.find('SYMBOL'), 'SYMBOL'),
              (row_fmt.find('TO_BASE'), 'TO_BASE'),
              (row_fmt.find('FROM_BASE'), 'FROM_BASE')
          )), start=1)}

    dates = []
    rates = []

    for message in mailbox.mbox(path):

        date_and_time = None
        to_base = None
        from_base = None

        for part in message.walk():

            content_type = part.get_content_type()
            if content_type.startswith('text/'):

                body = part.get_payload()

                # Render all text parts because sometimes HTML gets stuffed
                # into a plain text body one way or another:
                # parser = StripHTML()
                # parser.feed(body)
                # body = parser.content()
                body = BeautifulSoup(body, 'lxml').get_text(' ')  # space sep.

                for line in body.splitlines():

                    if not date_and_time:
                        match = dt_re.search(line)
                        if match:
                            d, t, z = match.groups()
                            d = re.sub(date_sep, '-', d)  # standardize format
                            date_and_time = dt.strptime(' '.join((d, t, z)),
                                                        '%Y-%m-%d %H:%M:%S %Z')

                    if from_c in line or to_c in line:  # faster than re
                        match = rates_re.search(line)
                        if match:
                            # TODO: parse the order
                            currency = match.group(pos['SYMBOL'])
                            if currency == from_c:
                                to_base = float(match.group(pos['TO_BASE']))
                            elif currency == to_c:
                                from_base = float(match.group(pos['FROM_BASE']))

        if date_and_time and to_base and from_base:
            dates.append(date_and_time)
            rates.append(to_base * from_base)

    # Supposedly it's more efficient to create a DataFrame from lists than to
    # append to one in a loop; it might create a temporary copy, but that
    # shouldn't use more than a few MB for a few decades of data:
    df = pd.DataFrame(rates, index=dates, columns=['{}/{}'.format(to_c, from_c)])
    df = df.sort_index()
    # df.rename_axis('UTC')
    return(df)

