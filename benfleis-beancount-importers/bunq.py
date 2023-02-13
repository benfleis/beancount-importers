#!/usr/bin/env python

import csv
from datetime import datetime
import logging
import os
import re

from beancount.core import account
from beancount.core import amount
from beancount.core import flags
from beancount.core import data
from beancount.core.number import D
from beancount.core.position import Cost
from beancount.ingest import importer

# starting point was mterwill's gist: https://gist.github.com/mterwill/7fdcc573dc1aa158648aacd4e33786e8

class BunqImporter(importer.ImporterProtocol):
    match_re = re.compile(r'''
        ^(.*/)?
        \d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_bunq-statement[.]csv
        $
        ''',
        flags=re.VERBOSE)

    def __init__(self, root, *subs):
        self.root = root
        self.accounts = set([root] + list(subs))
        self.accounts_by_iban = { a.iban: a for a in self.accounts }

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
            try:
                date = datetime.fromisoformat(row['Date']).date()
                # using locale is a PITA, do the hack.
                amount_  = row['Amount'] \
                    .replace(',', ' ') \
                    .replace('.', ',') \
                    .replace(' ', '.')
                account = self.accounts_by_iban[row['Account']]
                posting = data.Posting(
                    account.bean, amount.Amount(D(amount_), account.currency),
                    None, None, None, None)
                # counterparty = accounts_by_iban.get(row.get('Counterparty'))
                # TODO do counterparty stuff. specifically for bunq at least, note a transfer between sub
                # accounts.

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
            except:
                logging.error(f'failed on row: {row}')
                raise

        return [row_to_txn(row) for row in rows()]
