#!/usr/bin/env python

import csv
from datetime import datetime
import itertools
import logging
import re

from beancount.core import amount
from beancount.core import flags
from beancount.core import data
from beancount.core.number import D
# from beancount.core.position import Cost
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
    def __init__(self, accounts_by_id):
        self.accounts_by_id = accounts_by_id

    def identify(self, file):
        for id, acct in self.accounts_by_id.items():
            m = acct.file_match_re and acct.file_match_re.match(file.name)
            if m and m.groupdict()['id'] == id and acct.institution == 'ASN':
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
                date = datetime.strptime(row['Boekingsdatum'], '%d-%m-%Y').date()
                currency = row['Valutasoort rekening']
                units = D(row['Transactiebedrag'].replace(',', '.'))
                acct = self.accounts_by_id[row['Opdrachtgeversrekening']]
                counterparty = row['Tegenrekeningnummer']
                counterparty_acct = self.accounts_by_id.get(counterparty)

                assert currency == 'EUR'
                assert currency == acct.currency

                postings = [
                    data.Posting(acct.bean, amount.Amount(units, acct.currency), None, None, None, None),
                ]
                if counterparty_acct:
                    assert counterparty_acct.currency == acct.currency # TODO allow multi-currency event.
                    postings.append(
                        data.Posting(counterparty_acct.bean, amount.Amount(-units, acct.currency), None, None, None, None)
                    )

                txn = data.Transaction(
                    meta=row['_meta'],
                    date=date,
                    flag=flags.FLAG_TRANSFER if counterparty_acct else flags.FLAG_OKAY,
                    payee=row['Naam tegenrekening'] or row['Tegenrekeningnummer'],
                    narration=row['Omschrijving'],
                    tags=set(),
                    links=set(),
                    postings=postings,
                )
                return txn
            except:
                logging.error(f'failed on row: {row}')
                raise

        return [row_to_txn(row) for row in rows()]
