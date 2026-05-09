import sqlite3
import random
import os
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash

# Absolute path so the DB is always next to database.py regardless of
# the working directory (important for PythonAnywhere / WSGI servers).
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bank.db')


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            sort_code TEXT NOT NULL UNIQUE,
            account_number TEXT NOT NULL UNIQUE,
            bank_type TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
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

    for migration in [
        'ALTER TABLE transactions ADD COLUMN exported_at TEXT',
        'ALTER TABLE accounts ADD COLUMN user_id INTEGER',
    ]:
        try:
            conn.execute(migration)
            conn.commit()
        except Exception:
            pass

    if not conn.execute("SELECT id FROM users WHERE email = 'james@apex27.co.uk'").fetchone():
        conn.execute(
            'INSERT INTO users (email, password_hash, is_admin, created_at) VALUES (?, ?, 1, ?)',
            ('james@apex27.co.uk', generate_password_hash('apex27bank!'), datetime.now().isoformat())
        )
        conn.commit()

    admin = conn.execute("SELECT id FROM users WHERE email = 'james@apex27.co.uk'").fetchone()
    if admin:
        conn.execute('UPDATE accounts SET user_id = ? WHERE user_id IS NULL', (admin['id'],))
        conn.commit()

    conn.close()


# --- User functions ---

def get_user_by_email(email):
    conn = get_db()
    row = conn.execute('SELECT * FROM users WHERE email = ?', (email.lower().strip(),)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_user_by_id(user_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def authenticate_user(email, password):
    user = get_user_by_email(email)
    if user and check_password_hash(user['password_hash'], password):
        return user
    return None


def get_all_users():
    conn = get_db()
    rows = conn.execute('''
        SELECT u.*, COUNT(a.id) AS account_count
        FROM users u
        LEFT JOIN accounts a ON a.user_id = u.id
        GROUP BY u.id
        ORDER BY u.email
    ''').fetchall()
    conn.close()
    return [dict(r) for r in rows]


def create_user(email, password, is_admin=False):
    conn = get_db()
    conn.execute(
        'INSERT INTO users (email, password_hash, is_admin, created_at) VALUES (?, ?, ?, ?)',
        (email.lower().strip(), generate_password_hash(password), 1 if is_admin else 0, datetime.now().isoformat())
    )
    conn.commit()
    user_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    conn.close()
    return user_id


def delete_user(user_id):
    conn = get_db()
    conn.execute('DELETE FROM users WHERE id = ?', (user_id,))
    conn.commit()
    conn.close()


# --- Account functions ---

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


def get_accounts(user_id):
    conn = get_db()
    rows = conn.execute('''
        SELECT a.*,
               COALESCE(SUM(t.amount), 0.0) AS balance,
               COUNT(t.id) AS transaction_count,
               MAX(t.date) AS last_transaction_iso
        FROM accounts a
        LEFT JOIN transactions t ON t.account_id = a.id
        WHERE a.user_id = ?
        GROUP BY a.id
        ORDER BY a.name
    ''', (user_id,)).fetchall()
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


def create_account(name, bank_type, user_id, sort_code=None, account_number=None):
    conn = get_db()
    if sort_code is None:
        sort_code = _random_sort_code(conn)
    if account_number is None:
        account_number = _random_account_number(conn)
    conn.execute(
        'INSERT INTO accounts (user_id, name, sort_code, account_number, bank_type, created_at) VALUES (?, ?, ?, ?, ?, ?)',
        (user_id, name, sort_code, account_number, bank_type, datetime.now().isoformat())
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


def find_account_by_details(sort_code, account_number, user_id):
    conn = get_db()
    row = conn.execute(
        'SELECT * FROM accounts WHERE sort_code = ? AND account_number = ? AND user_id = ?',
        (sort_code.strip(), account_number.strip(), user_id)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def reset_all_transactions(user_id):
    conn = get_db()
    conn.execute('''
        DELETE FROM transactions
        WHERE account_id IN (SELECT id FROM accounts WHERE user_id = ?)
    ''', (user_id,))
    conn.commit()
    conn.close()


# --- Transaction functions ---

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
