from flask import Flask, render_template, request, redirect, url_for, flash, make_response, abort
import os
import database as db
import exporters
import importers

app = Flask(__name__)
app.secret_key = os.urandom(24)

db.init_db()


@app.route('/')
def dashboard():
    accounts = db.get_accounts()
    return render_template('dashboard.html', accounts=accounts)


@app.route('/account/create', methods=['POST'])
def create_account():
    name = request.form.get('name', '').strip()
    bank_type = request.form.get('bank_type', '').strip()
    if not name:
        flash('Account name is required.', 'danger')
        return redirect(url_for('dashboard'))
    if bank_type not in ('barclays', 'natwest', 'sage'):
        flash('Please select a valid bank type.', 'danger')
        return redirect(url_for('dashboard'))
    account_id, sort_code, account_number = db.create_account(name, bank_type)
    flash(f'Account created — Sort Code: {sort_code} &nbsp;|&nbsp; Account Number: {account_number}', 'success')
    return redirect(url_for('account_detail', account_id=account_id))


@app.route('/account/<int:account_id>')
def account_detail(account_id):
    account = db.get_account(account_id)
    if not account:
        abort(404)
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    transactions = db.get_transactions_with_balance(
        account_id,
        date_from or None,
        date_to or None,
    )
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
def add_transaction(account_id):
    account = db.get_account(account_id)
    if not account:
        abort(404)
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
def delete_transaction(account_id, transaction_id):
    db.delete_transaction(transaction_id)
    flash('Transaction deleted.', 'success')
    return redirect(url_for('account_detail', account_id=account_id))


@app.route('/account/<int:account_id>/transaction/<int:transaction_id>/reset-export', methods=['POST'])
def reset_transaction_export(account_id, transaction_id):
    db.reset_transaction_export(transaction_id)
    return redirect(url_for('account_detail', account_id=account_id))


@app.route('/account/<int:account_id>/export')
def export_statement(account_id):
    account = db.get_account(account_id)
    if not account:
        abort(404)
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
def delete_account(account_id):
    account = db.get_account(account_id)
    if account:
        db.delete_account(account_id)
        flash(f'Account "{account["name"]}" deleted.', 'success')
    return redirect(url_for('dashboard'))


@app.route('/import', methods=['GET', 'POST'])
def import_bacs():
    if request.method == 'GET':
        return render_template('import_bacs.html')

    bacs_format = request.form.get('format', '')
    file = request.files.get('file')

    if not file or not file.filename:
        flash('Please select a file to import.', 'danger')
        return render_template('import_bacs.html')
    if bacs_format not in ('barclays', 'natwest', 'sage'):
        flash('Please select a BACS format.', 'danger')
        return render_template('import_bacs.html')

    content = file.read().decode('utf-8-sig')

    if bacs_format == 'barclays':
        parsed = importers.import_barclays(content)
    elif bacs_format == 'natwest':
        parsed = importers.import_natwest(content)
    else:
        parsed = importers.import_sage(content)

    matched = []
    unmatched = []
    for t in parsed:
        account = db.find_account_by_details(t['sort_code'], t['account_number'])
        if account:
            db.add_transaction(
                account['id'], t['date'], t['description'],
                t['amount'], t['reference'], 'BACS',
            )
            matched.append({**t, 'account_name': account['name']})
        else:
            unmatched.append(t)

    return render_template(
        'import_bacs.html',
        matched=matched,
        unmatched=unmatched,
        processed=True,
        bacs_format=bacs_format,
    )


if __name__ == '__main__':
    app.run(debug=True, port=5000)
