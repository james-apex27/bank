import csv
import io
from datetime import datetime


def _parse_date(value):
    value = str(value).strip().strip("'\" ")
    for fmt in ('%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d', '%d/%m/%y'):
        try:
            return datetime.strptime(value, fmt).strftime('%d/%m/%Y')
        except ValueError:
            continue
    return datetime.now().strftime('%d/%m/%Y')


def _parse_amount(value):
    value = str(value).strip().strip("'\"£, ")
    if not value:
        return 0.0
    return float(value.replace(',', ''))


def _parse_rows(file_content):
    content = file_content.strip()
    if not content:
        return []
    reader = csv.DictReader(io.StringIO(content))
    try:
        rows = list(reader)
        return rows
    except Exception:
        return []


def _normalise(row):
    return {k.lower().strip(): str(v).strip().strip("'\" ") for k, v in row.items() if k}


def import_barclays(file_content):
    transactions = []
    for row in _parse_rows(file_content):
        r = _normalise(row)
        sort_code = r.get('sort code', r.get('sortcode', ''))
        account_number = r.get('account number', r.get('accountnumber', r.get('account_number', '')))
        name = r.get('beneficiary name', r.get('beneficiary', r.get('name', '')))
        amount = _parse_amount(r.get('amount', '0'))
        reference = r.get('reference', r.get('payment reference', ''))
        date = _parse_date(r.get('payment date', r.get('date', datetime.now().strftime('%d/%m/%Y'))))
        if sort_code and account_number:
            transactions.append({
                'sort_code': sort_code,
                'account_number': account_number,
                'beneficiary_name': name,
                'amount': amount,
                'reference': reference,
                'date': date,
                'description': f"BACS from Apex27{' — ' + name if name else ''}",
            })
    return transactions


def import_natwest(file_content):
    transactions = []
    for row in _parse_rows(file_content):
        r = _normalise(row)
        sort_code = r.get('destination sort code', r.get('sort code', r.get('sortcode', '')))
        account_number = r.get('destination account number', r.get('account number', r.get('accountnumber', '')))
        name = r.get('beneficiary name', r.get('name', ''))
        amount = _parse_amount(r.get('amount', '0'))
        reference = r.get('reference', '')
        date = _parse_date(r.get('payment date', r.get('date', datetime.now().strftime('%d/%m/%Y'))))
        if sort_code and account_number:
            transactions.append({
                'sort_code': sort_code,
                'account_number': account_number,
                'beneficiary_name': name,
                'amount': amount,
                'reference': reference,
                'date': date,
                'description': f"BACS from Apex27{' — ' + name if name else ''}",
            })
    return transactions


def import_barclays_bacs(file_content):
    transactions = []
    for line in file_content.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(',')]
        if len(parts) < 4:
            continue
        raw_sc = parts[0].replace('-', '')
        if len(raw_sc) == 6:
            sort_code = f"{raw_sc[0:2]}-{raw_sc[2:4]}-{raw_sc[4:6]}"
        else:
            sort_code = raw_sc
        name = parts[1]
        account_number = parts[2]
        raw_amount = parts[3].strip()
        amount = _parse_amount(raw_amount)
        if '.' not in raw_amount:
            amount = amount / 100
        reference = parts[4] if len(parts) > 4 else ''
        transactions.append({
            'sort_code': sort_code,
            'account_number': account_number,
            'beneficiary_name': name,
            'amount': amount,
            'reference': reference,
            'date': datetime.now().strftime('%d/%m/%Y'),
            'description': f"BACS from Apex27{' — ' + name if name else ''}",
        })
    return transactions


def import_sage(file_content):
    transactions = []
    for row in _parse_rows(file_content):
        r = _normalise(row)
        sort_code = r.get('destination sort code', r.get('sort code', ''))
        account_number = r.get('destination account number', r.get('account number', ''))
        name = r.get('account name', r.get('beneficiary name', r.get('name', '')))
        amount = _parse_amount(r.get('amount', '0'))
        reference = r.get('reference', '')
        date = _parse_date(r.get('date', datetime.now().strftime('%d/%m/%Y')))
        if sort_code and account_number:
            transactions.append({
                'sort_code': sort_code,
                'account_number': account_number,
                'beneficiary_name': name,
                'amount': amount,
                'reference': reference,
                'date': date,
                'description': f"BACS from Apex27{' — ' + name if name else ''}",
            })
    return transactions
