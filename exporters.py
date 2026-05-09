import csv
import io

NATWEST_TYPE_MAP = {
    'Faster Payment': 'FPI',
    'BACS': 'BACS',
    'Standing Order': 'SO',
    'Direct Debit': 'DD',
    'CHAPS': 'CHAPS',
    'Card Payment': 'POS',
    'Credit': 'CR',
    'Debit': 'DR',
}


def export_barclays(account, transactions):
    out = io.StringIO()
    w = csv.writer(out, lineterminator='\r\n')
    w.writerow(['Number', 'Date', 'Account', 'Amount', 'Subcategory', 'Memo'])
    for i, t in enumerate(transactions, 1):
        subcategory = 'FASTER PAYMENTS RECEIPT' if t['amount'] >= 0 else 'FASTER PAYMENTS PAYMENT'
        memo = t['description']
        if t['reference']:
            memo = f"{t['description']} - {t['reference']}"
        w.writerow([i, t['date'], f"'{account['sort_code']}'", t['amount'], f"'{subcategory}'", f"'{memo}'"])
    return out.getvalue()


def export_natwest(account, transactions):
    out = io.StringIO()
    w = csv.writer(out, lineterminator='\r\n')
    w.writerow(['Date', 'Type', 'Description', 'Value', 'Balance', 'Account Name', 'Account Number'])
    for t in transactions:
        type_code = NATWEST_TYPE_MAP.get(t['transaction_type'], 'CR' if t['amount'] >= 0 else 'DR')
        description = t['description']
        if t['reference']:
            description = f"{description} {t['reference']}"
        w.writerow([
            t['date'], type_code, description,
            t['amount'], f"{t['running_balance']:.2f}",
            account['name'], account['account_number'],
        ])
    return out.getvalue()


def export_sage(account, transactions):
    out = io.StringIO()
    w = csv.writer(out, quoting=csv.QUOTE_ALL, lineterminator='\r\n')
    w.writerow(['Date', 'Reference', 'Description', 'Receipts', 'Payments', 'Balance'])
    for t in transactions:
        receipts = f"{t['amount']:.2f}" if t['amount'] > 0 else ''
        payments = f"{abs(t['amount']):.2f}" if t['amount'] < 0 else ''
        w.writerow([
            t['date'], t['reference'] or '', t['description'],
            receipts, payments, f"{t['running_balance']:.2f}",
        ])
    return out.getvalue()
