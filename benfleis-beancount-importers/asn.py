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

# ASN CSV spec: https://www.asnbank.nl/web/file?uuid=fc28db9c-d91e-4a2c-bd3a-30cffb057e8b&owner=6916ad14-918d-4ea8-80ac-f71f0ff1928e&contentid=852

field_names = [
    "Boekingsdatum",
    "Opdrachtgeversrekening",
    "Tegenrekeningnummer",
    "Naam tegenrekening",
    "_Adres",
    "_Postcode",
    "_Plaats",
    "Valutasoort rekening",
    "Saldo rekening voor mutatie",
    "Valutasoort mutatie",
    "Transactiebedrag",
    "Journaaldatum",
    "Valutadatum",
    "Interne transactiecode",
    "Globale transactiecode",
    "Volgnummer transactie",
    "Betalingskenmerk",
    "Omschrijving",
    "Afschriftnummer",
]

class ASNImporter(importer.ImporterProtocol):
    match_re = re.compile(r'''
        ^(.*/)?
        (?P<account>\d{10})_(?P<date>\d{8})_\d{6}[.]csv
        $
        ''',
        flags=re.VERBOSE)

    def __init__(self, root, *subs):
        self.root = root
        self.accounts = set([root] + list(subs))
        self.accounts_by_iban = { a.iban: a for a in self.accounts }

    def identify(self, f):
        m = ASNImporter.match_re.match(f.name)
        return m and m.groupdict()['account'] in self.root.iban

    def file_name(self, file):
        return 'asn.{}'.format(path.basename(file.name))

    def file_account(self, _):
        return self.root.bean

    def file_date(self, file):
        date_str = ASNImporter.match_re.match(file.name).groupdict()['date']
        return datetime.strptime(date_str, '%d%m%Y').date()

    def extract(self, file):
        def rows():
            with open(file.name) as f:
                reader = csv.DictReader(f, fieldnames=field_names, quotechar="'")
                for index, row in enumerate(reader):
                  row['_meta'] = data.new_metadata(file.name, index)
                  yield row

        def row_to_txn(row):
            try:
                date = datetime.strptime(row['Boekingsdatum'], '%d-%m-%Y').date()
                amount_  = row['Transactiebedrag'].replace(',', '.')
                account = self.accounts_by_iban[row['Opdrachtgeversrekening']]
                posting = data.Posting(
                    account.bean, amount.Amount(D(amount_), account.currency),
                    None, None, None, None)
                counterparty = row['Tegenrekeningnummer'] # accounts_by_iban.get(row.get('Counterparty'))
                # TODO do counterparty stuff
                # accounts.

                assert row['Valutasoort rekening'] == 'EUR'

                txn = data.Transaction(
                    meta=row['_meta'],
                    date=date,
                    flag=flags.FLAG_OKAY,
                    payee=row['Naam tegenrekening'] or row['Tegenrekeningnummer'],
                    narration=row['Omschrijving'],
                    tags=set(),
                    links=set(),
                    postings=[posting],
                )
                return txn
            except:
                logging.error(f'failed on row: {row}')
                raise

        return [row_to_txn(row) for row in rows()]
