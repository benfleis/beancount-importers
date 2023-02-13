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
    def __init__(self, accounts_by_id):
        self.accounts_by_id = accounts_by_id
        self.bunq_account_ids = {id for id, acct in accounts_by_id.items() if acct.institution == 'Bunq'} 

    def identify(self, file):
        # check known account-specific matchers
        for id, acct in self.accounts_by_id.items():
            m = acct.file_match_re and acct.file_match_re.match(file.name)
            if m and m.groupdict()['id'] == id and acct.institution == 'Bunq':
                return m
        return None


    def file_date(self, file):
        m = self.identify(file)
        assert(m != None)
        return datetime.strptime(m['end_date'], '%Y-%m-%d').date()

    def extract(self, file):
        m = self.identify(file)
        assert(m != None)

        def rows():
            with open(file.name) as f:
                reader = csv.DictReader(f, delimiter=';', quotechar='"')
                for index, row in enumerate(reader):
                  row['_meta'] = data.new_metadata(file.name, index)
                  yield row

        def row_to_txn(row):
            try:
                acct = self.accounts_by_id[row['Account']]
                date = datetime.fromisoformat(row['Date']).date()
                # using locale is a PITA, do the hack.
                units = D(row['Amount'].replace(',', ' ').replace('.', ',').replace(' ', '.'))
                desc = row['Description']
                # TODO
                # example row with Bunq's currency conversion
                #  "2022-03-09";"2022-04-01";"-20,72";"XXXX";"";"UBER * PENDING";"UBER * PENDING help.uber.com, NL 22.36 USD, 1 USD = 0.92665 EUR"
                postings = [
                    data.Posting(acct.bean, amount.Amount(units, acct.currency), None, None, None, None)
                ]
                counterparty = row.get('Counterparty')
                counterparty_acct = self.accounts_by_id.get(counterparty)

                if counterparty_acct:
                    assert counterparty_acct.currency == acct.currency # TODO allow multi-currency event.
                    postings.append(
                        data.Posting(counterparty_acct.bean, amount.Amount(-units, acct.currency), None, None, None, None)
                    )
                txn = data.Transaction(
                    meta=row['_meta'],
                    date=date,
                    flag=flags.FLAG_TRANSFER if counterparty_acct else flags.FLAG_OKAY,
                    payee=row['Name'] or counterparty,
                    narration=desc,
                    tags=set(),
                    links=set(),
                    postings=postings,
                )
                return txn
            except:
                logging.error(f'failed on row: {row}')
                raise

        return [row_to_txn(row) for row in rows()]
