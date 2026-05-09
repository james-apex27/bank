import sqlite3
import random
from datetime import datetime

DB_PATH = 'bank.db'


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            sort_code TEXT NOT NULL UNIQUE,
            account_number TEXT NOT NULL UNIQUE,
            bank_type TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            description TEXT NOT NULL,
            amount REAL NOT NULL,
            reference TEXT NOT NULL DEFAULT '',
            transaction_type TEXT NOT NULL DEFAULT 'Faster Payment',
            created_at TEXT NOT NULL,
            exported_at TEXT,
            FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
        );
    ''')
    conn.commit()
    # Migrate existing DB if exported_at column is missing
    try:
        conn.execute('ALTER TABLE transactions ADD COLUMN exported_at TEXT')
        conn.commit()
    except Exception:
        pass
    conn.close()


def _random_sort_code(conn):
    while True:
        sc = f"{random.randint(10, 99):02d}-{random.randint(10, 99):02d}-{random.randint(10, 99):02d}"
        if not conn.execute('SELECT id FROM accounts WHERE sort_code = ?', (sc,)).fetchone():
            return sc


def _random_account_number(conn):
    while True:
        an = str(random.randint(10000000, 99999999))
        if not conn.execute('SELECT id FROM accounts WHERE account_number = ?', (an,)).fetchone():
            return an


def get_accounts():
    conn = get_db()
    rows = conn.execute('''
        SELECT a.*,
               COALESCE(SUM(t.amount), 0.0) AS balance,
               COUNT(t.id) AS transaction_count,
               MAX(t.date) AS last_transaction_iso
        FROM accounts a
        LEFT JOIN transactions t ON t.account_id = a.id
        GROUP BY a.id
        ORDER BY a.name
    ''').fetchall()
    conn.close()
    accounts = []
    for row in rows:
        d = dict(row)
        if d['last_transaction_iso']:
            try:
                d['last_transaction'] = datetime.strptime(d['last_transaction_iso'], '%Y-%m-%d').strftime('%d/%m/%Y')
            except ValueError:
                d['last_transaction'] = d['last_transaction_iso']
        else:
            d['last_transaction'] = None
        accounts.append(d)
    return accounts


def get_account(account_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM accounts WHERE id = ?', (account_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def create_account(name, bank_type):
    conn = get_db()
    sort_code = _random_sort_code(conn)
    account_number = _random_account_number(conn)
    conn.execute(
        'INSERT INTO accounts (name, sort_code, account_number, bank_type, created_at) VALUES (?, ?, ?, ?, ?)',
        (name, sort_code, account_number, bank_type, datetime.now().isoformat())
    )
    conn.commit()
    account_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    conn.close()
    return account_id, sort_code, account_number


def delete_account(account_id):
    conn = get_db()
    conn.execute('DELETE FROM accounts WHERE id = ?', (account_id,))
    conn.commit()
    conn.close()


def get_transactions_with_balance(account_id, date_from=None, date_to=None):
    conn = get_db()
    query = 'SELECT * FROM transactions WHERE account_id = ?'
    params = [account_id]
    if date_from:
        try:
            query += ' AND date >= ?'
            params.append(datetime.strptime(date_from, '%d/%m/%Y').strftime('%Y-%m-%d'))
        except ValueError:
            pass
    if date_to:
        try:
            query += ' AND date <= ?'
            params.append(datetime.strptime(date_to, '%d/%m/%Y').strftime('%Y-%m-%d'))
        except ValueError:
            pass
    query += ' ORDER BY date ASC, id ASC'
    rows = conn.execute(query, params).fetchall()
    conn.close()

    result = []
    running_balance = 0.0
    for row in rows:
        running_balance += row['amount']
        result.append({
            'id': row['id'],
            'date': datetime.strptime(row['date'], '%Y-%m-%d').strftime('%d/%m/%Y'),
            'description': row['description'],
            'amount': row['amount'],
            'reference': row['reference'],
            'transaction_type': row['transaction_type'],
            'running_balance': running_balance,
            'exported_at': row['exported_at'],
        })
    return result


def get_unexported_transactions_with_balance(account_id):
    """Returns only unexported transactions, but running_balance includes all prior transactions."""
    conn = get_db()
    all_rows = conn.execute(
        'SELECT * FROM transactions WHERE account_id = ? ORDER BY date ASC, id ASC',
        (account_id,)
    ).fetchall()
    conn.close()
    result = []
    running_balance = 0.0
    for row in all_rows:
        running_balance += row['amount']
        if row['exported_at'] is None:
            result.append({
                'id': row['id'],
                'date': datetime.strptime(row['date'], '%Y-%m-%d').strftime('%d/%m/%Y'),
                'description': row['description'],
                'amount': row['amount'],
                'reference': row['reference'],
                'transaction_type': row['transaction_type'],
                'running_balance': running_balance,
            })
    return result


def get_unexported_count(account_id):
    conn = get_db()
    row = conn.execute(
        'SELECT COUNT(*) FROM transactions WHERE account_id = ? AND exported_at IS NULL',
        (account_id,)
    ).fetchone()
    conn.close()
    return row[0] if row else 0


def mark_transactions_exported(transaction_ids):
    if not transaction_ids:
        return
    conn = get_db()
    placeholders = ','.join('?' * len(transaction_ids))
    conn.execute(
        f'UPDATE transactions SET exported_at = ? WHERE id IN ({placeholders})',
        [datetime.now().isoformat()] + list(transaction_ids)
    )
    conn.commit()
    conn.close()


def reset_transaction_export(transaction_id):
    conn = get_db()
    conn.execute('UPDATE transactions SET exported_at = NULL WHERE id = ?', (transaction_id,))
    conn.commit()
    conn.close()


def add_transaction(account_id, date_str, description, amount, reference, transaction_type):
    date_iso = datetime.strptime(date_str.strip(), '%d/%m/%Y').strftime('%Y-%m-%d')
    conn = get_db()
    conn.execute(
        '''INSERT INTO transactions
           (account_id, date, description, amount, reference, transaction_type, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)''',
        (account_id, date_iso, description, float(amount), reference or '', transaction_type,
         datetime.now().isoformat())
    )
    conn.commit()
    conn.close()


def delete_transaction(transaction_id):
    conn = get_db()
    conn.execute('DELETE FROM transactions WHERE id = ?', (transaction_id,))
    conn.commit()
    conn.close()


def find_account_by_details(sort_code, account_number):
    conn = get_db()
    row = conn.execute(
        'SELECT * FROM accounts WHERE sort_code = ? AND account_number = ?',
        (sort_code.strip(), account_number.strip())
    ).fetchone()
    conn.close()
    return dict(row) if row else None
