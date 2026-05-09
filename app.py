from flask import Flask, render_template, request, redirect, url_for, flash, make_response, abort, session
from functools import wraps
import os
import database as db
import exporters
import importers

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'apex27-bank-simulator-dev-key')

db.init_db()


# --- Auth helpers ---

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login', next=request.path))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login', next=request.path))
        if not session.get('is_admin'):
            abort(403)
        return f(*args, **kwargs)
    return decorated


def current_user_id():
    return session['user_id']


def _own_account(account_id):
    account = db.get_account(account_id)
    if not account or account['user_id'] != current_user_id():
        abort(404)
    return account


# --- Auth routes ---

@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'user_id' in session:
        return redirect(url_for('dashboard'))
    if request.method == 'POST':
        email = request.form.get('email', '').strip()
        password = request.form.get('password', '')
        user = db.authenticate_user(email, password)
        if user:
            session['user_id'] = user['id']
            session['user_email'] = user['email']
            session['is_admin'] = bool(user['is_admin'])
            next_url = request.form.get('next') or url_for('dashboard')
            return redirect(next_url)
        flash('Invalid email or password.', 'danger')
    return render_template('login.html', next=request.args.get('next', ''))


@app.route('/logout', methods=['POST'])
def logout():
    session.clear()
    return redirect(url_for('login'))


# --- Dashboard ---

@app.route('/')
@login_required
def dashboard():
    accounts = db.get_accounts(current_user_id())
    return render_template('dashboard.html', accounts=accounts)


@app.route('/reset-transactions', methods=['POST'])
@login_required
def reset_transactions():
    db.reset_all_transactions(current_user_id())
    flash('All transactions cleared.', 'success')
    return redirect(url_for('dashboard'))


# --- Accounts ---

@app.route('/account/create', methods=['POST'])
@login_required
def create_account():
    name = request.form.get('name', '').strip()
    bank_type = request.form.get('bank_type', '').strip()
    if not name:
        flash('Account name is required.', 'danger')
        return redirect(url_for('dashboard'))
    if bank_type not in ('barclays', 'natwest', 'sage'):
        flash('Please select a valid bank type.', 'danger')
        return redirect(url_for('dashboard'))
    account_id, sort_code, account_number = db.create_account(name, bank_type, current_user_id())
    flash(f'Account created — Sort Code: {sort_code} &nbsp;|&nbsp; Account Number: {account_number}', 'success')
    return redirect(url_for('account_detail', account_id=account_id))


@app.route('/account/<int:account_id>')
@login_required
def account_detail(account_id):
    account = _own_account(account_id)
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    transactions = db.get_transactions_with_balance(account_id, date_from or None, date_to or None)
    balance = transactions[-1]['running_balance'] if transactions else 0.0
    unexported_count = db.get_unexported_count(account_id)
    return render_template(
        'account.html',
        account=account,
        transactions=transactions,
        balance=balance,
        date_from=date_from,
        date_to=date_to,
        unexported_count=unexported_count,
    )


@app.route('/account/<int:account_id>/transaction/add', methods=['POST'])
@login_required
def add_transaction(account_id):
    account = _own_account(account_id)
    date = request.form.get('date', '').strip()
    description = request.form.get('description', '').strip()
    amount = request.form.get('amount', '').strip()
    reference = request.form.get('reference', '').strip()
    transaction_type = request.form.get('transaction_type', 'Faster Payment')
    if not date or not description or not amount:
        flash('Date, description and amount are required.', 'danger')
        return redirect(url_for('account_detail', account_id=account_id))
    try:
        db.add_transaction(account_id, date, description, float(amount), reference, transaction_type)
        flash('Transaction added.', 'success')
    except (ValueError, Exception) as e:
        flash(f'Error: {e}', 'danger')
    return redirect(url_for('account_detail', account_id=account_id))


@app.route('/account/<int:account_id>/transaction/<int:transaction_id>/delete', methods=['POST'])
@login_required
def delete_transaction(account_id, transaction_id):
    _own_account(account_id)
    db.delete_transaction(transaction_id)
    flash('Transaction deleted.', 'success')
    return redirect(url_for('account_detail', account_id=account_id))


@app.route('/account/<int:account_id>/transaction/<int:transaction_id>/reset-export', methods=['POST'])
@login_required
def reset_transaction_export(account_id, transaction_id):
    _own_account(account_id)
    db.reset_transaction_export(transaction_id)
    return redirect(url_for('account_detail', account_id=account_id))


@app.route('/account/<int:account_id>/export')
@login_required
def export_statement(account_id):
    account = _own_account(account_id)
    transactions = db.get_unexported_transactions_with_balance(account_id)
    if not transactions:
        flash('No new transactions to export — all are already marked as exported.', 'warning')
        return redirect(url_for('account_detail', account_id=account_id))
    bank_type = account['bank_type']
    if bank_type == 'barclays':
        content = exporters.export_barclays(account, transactions)
        filename = f"barclays_statement_{account['account_number']}.csv"
    elif bank_type == 'natwest':
        content = exporters.export_natwest(account, transactions)
        filename = f"natwest_statement_{account['account_number']}.csv"
    else:
        content = exporters.export_sage(account, transactions)
        filename = f"sage_statement_{account['account_number']}.csv"
    ids = [t['id'] for t in transactions]
    db.mark_transactions_exported(ids)
    response = make_response(content)
    response.headers['Content-Type'] = 'text/csv; charset=utf-8'
    response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response


@app.route('/account/<int:account_id>/delete', methods=['POST'])
@login_required
def delete_account(account_id):
    account = _own_account(account_id)
    db.delete_account(account_id)
    flash(f'Account "{account["name"]}" deleted.', 'success')
    return redirect(url_for('dashboard'))


# --- BACS Import ---

@app.route('/account/<int:account_id>/import', methods=['GET', 'POST'])
@login_required
def import_bacs(account_id):
    source = _own_account(account_id)

    if request.method == 'GET':
        return render_template('import_bacs.html', source=source)

    bacs_format = request.form.get('format', '')
    file = request.files.get('file')

    if not file or not file.filename:
        flash('Please select a file to import.', 'danger')
        return render_template('import_bacs.html', source=source)
    if bacs_format not in ('barclays', 'natwest', 'sage'):
        flash('Please select a BACS format.', 'danger')
        return render_template('import_bacs.html', source=source)

    content = file.read().decode('utf-8-sig')

    if bacs_format == 'barclays':
        parsed = importers.import_barclays(content)
    elif bacs_format == 'natwest':
        parsed = importers.import_natwest(content)
    else:
        parsed = importers.import_sage(content)

    matched = []
    auto_created = []
    for t in parsed:
        account = db.find_account_by_details(t['sort_code'], t['account_number'], current_user_id())
        if not account:
            name = t['beneficiary_name'] or f"{t['sort_code']} / {t['account_number']}"
            try:
                new_id, _, _ = db.create_account(
                    name, bacs_format, current_user_id(),
                    sort_code=t['sort_code'], account_number=t['account_number']
                )
                account = db.get_account(new_id)
                auto_created.append(account['name'])
            except Exception:
                account = None

        if account:
            db.add_transaction(
                account['id'], t['date'], t['description'],
                t['amount'], t['reference'], 'BACS',
            )
            matched.append({**t, 'account_name': account['name']})

    if matched:
        total = sum(t['amount'] for t in matched)
        n = len(matched)
        db.add_transaction(
            account_id,
            matched[0]['date'],
            f"BACS run — {n} payment{'s' if n != 1 else ''}",
            -total,
            '',
            'BACS',
        )

    return render_template(
        'import_bacs.html',
        source=source,
        matched=matched,
        auto_created=auto_created,
        processed=True,
        bacs_format=bacs_format,
    )


# --- Admin: User Management ---

@app.route('/admin/users')
@admin_required
def admin_users():
    users = db.get_all_users()
    return render_template('admin_users.html', users=users)


@app.route('/admin/users/create', methods=['POST'])
@admin_required
def admin_create_user():
    email = request.form.get('email', '').strip()
    password = request.form.get('password', '').strip()
    is_admin = bool(request.form.get('is_admin'))
    if not email or not password:
        flash('Email and password are required.', 'danger')
        return redirect(url_for('admin_users'))
    if db.get_user_by_email(email):
        flash(f'{email} already exists.', 'danger')
        return redirect(url_for('admin_users'))
    db.create_user(email, password, is_admin)
    flash(f'User {email} created.', 'success')
    return redirect(url_for('admin_users'))


@app.route('/admin/users/<int:user_id>/delete', methods=['POST'])
@admin_required
def admin_delete_user(user_id):
    if user_id == current_user_id():
        flash('You cannot delete your own account.', 'danger')
        return redirect(url_for('admin_users'))
    user = db.get_user_by_id(user_id)
    if user:
        db.delete_user(user_id)
        flash(f'User {user["email"]} deleted.', 'success')
    return redirect(url_for('admin_users'))


if __name__ == '__main__':
    app.run(debug=True, port=5000)
