#!/usr/bin/env python

import csv
from datetime import datetime
import logging
import os
import re
# from titlecase import titlecase

from beancount.core import account
from beancount.core import amount
from beancount.core import flags
from beancount.core import data
from beancount.core.number import D
from beancount.core.position import Cost
from beancount.ingest import importer


class BunqImporter(importer.ImporterProtocol):
    match_re = re.compile(r'''
        ^(.*/)?
        \d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_bunq-statement[.]csv
        $
        ''',
        flags=re.VERBOSE)

    def __init__(self, root, *accounts):
        self.root = root
        self.accounts = set([root] + list(accounts))
        self.accounts_by_iban = { a.iban: a for a in accounts }

    def identify(self, f):
        return BunqImporter.match_re.match(f.name)

    def file_name(self, file):
        return 'bunq.{}'.format(path.basename(file.name))

    def file_account(self, _):
        return self.root.bean

    #def file_date(self, file):
    #    # Extract the statement date from the filename.
    #    return datetime.datetime.strptime(path.basename(file.name),
    #                                      'UTrade%Y%m%d.csv').date()

    def extract(self, file):
        def rows():
            with open(file.name) as f:
                for index, row in enumerate(csv.DictReader(f, delimiter=';')):
                  row['_meta'] = data.new_metadata(file.name, index)
                  yield row

        def row_to_txn(row):
            logging.error(row)
            # import pdb; pdb.set_trace()
            date = datetime.fromisoformat(row['Date']).date()
            amount_  = row['Amount'].replace(',', '.', 1)
            account = self.accounts_by_iban[row['Account']]
            posting = data.Posting(
                account.bean, amount.Amount(D(amount_), account.currency),
                None, None, None, None)
            # counterparty = accounts_by_iban.get(row.get('Counterparty'))
            # do counterparty stuff.

            txn = data.Transaction(
                meta=row['_meta'],
                date=date,
                flag=flags.FLAG_OKAY,
                payee=row['Name'],
                narration=row['Description'],
                tags=set(),
                links=set(),
                postings=[posting],
            )
            return txn

        return [row_to_txn(row) for row in rows()]

#   def extract_(self, f):
#       entries = []

#       with open(f.name) as f:
#           for index, row in enumerate(csv.DictReader(f)):
#               trans_date = parse(row['Trans Date']).date()
#               trans_desc = titlecase(row['Description'])
#               trans_amt  = row['Amount']

#               meta = data.new_metadata(f.name, index)

#               txn = data.Transaction(
#                   meta=meta,
#                   date=trans_date,
#                   flag=flags.FLAG_OKAY,
#                   payee=trans_desc,
#                   narration="",
#                   tags=set(),
#                   links=set(),
#                   postings=[],
#               )

#               txn.postings.append(
#                   data.Posting(self.account, amount.Amount(D(trans_amt),
#                       'USD'), None, None, None, None)
#               )

#               entries.append(txn)

#       return entries

#   def extract__(self, f):
#       entries = []

#       with open(f.name) as f:
#           for index, row in enumerate(csv.DictReader(f)):
#               trans_date = parse(row['Posting Date']).date()
#               trans_desc = titlecase(row['Description'])
#               trans_amt  = row['Amount']

#               meta = data.new_metadata(f.name, index)

#               txn = data.Transaction(
#                   meta=meta,
#                   date=trans_date,
#                   flag=flags.FLAG_OKAY,
#                   payee=trans_desc,
#                   narration="",
#                   tags=set(),
#                   links=set(),
#                   postings=[],
#               )

#               txn.postings.append(
#                   data.Posting(self.account, amount.Amount(D(trans_amt),
#                       'USD'), None, None, None, None)
#               )

#               entries.append(txn)

#       return entries
