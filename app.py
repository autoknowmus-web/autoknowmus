# ============================================================
# BUYER SEARCH — Market Intelligence
# ============================================================
@app.route('/buyer', methods=['GET', 'POST'])
@login_required
def buyer():
    user_id = session.get('user_id')
    user_resp = supabase.table('users').select('*').eq('id', user_id).limit(1).execute()
    user = user_resp.data[0] if user_resp.data else None

    if not user:
        flash('Session expired. Please log in again.', 'error')
        return redirect(url_for('logout'))

    # Split first name for greeting (standing rule)
    first_name = (user.get('name') or 'there').split(' ')[0]

    def render_form(prefill):
        return render_template(
            'buyer.html',
            user=user,
            first_name=first_name,
            prefill=prefill,
            makes=sorted(CAR_DATA.keys()),
            fuels=['Petrol', 'Diesel', 'CNG', 'HEV', 'PHEV', 'BEV'],
            years=list(range(2026, 2010, -1)),
            conditions=['Excellent', 'Good', 'Fair'],
            car_data_json=json.dumps(CAR_DATA),
        )

    # ---- GET: render form (with optional prefill from URL query string) ----
    if request.method == 'GET':
        prefill = {
            'make': request.args.get('make', ''),
            'fuel': request.args.get('fuel', ''),
            'model': request.args.get('model', ''),
            'variant': request.args.get('variant', ''),
            'year': request.args.get('year', ''),
            'condition': request.args.get('condition', ''),
            'asking_price': request.args.get('asking_price', ''),
        }
        return render_form(prefill)

    # ---- POST: process buyer search ----
    form = request.form
    make = (form.get('make') or '').strip()
    fuel = (form.get('fuel') or '').strip()
    model = (form.get('model') or '').strip()
    variant = (form.get('variant') or '').strip()
    year = (form.get('year') or '').strip()
    condition = (form.get('condition') or '').strip()
    asking_price_raw = (form.get('asking_price') or '').strip()

    # Strip commas from asking price before int conversion
    asking_price = None
    if asking_price_raw:
        try:
            asking_price = int(asking_price_raw.replace(',', '').replace('₹', '').strip())
        except ValueError:
            asking_price = None

    # Validate required fields
    prefill = {
        'make': make, 'fuel': fuel, 'model': model, 'variant': variant,
        'year': year, 'condition': condition, 'asking_price': asking_price_raw,
    }
    if not all([make, fuel, model, variant, year, condition]):
        flash('Please fill in all required fields.', 'error')
        return render_form(prefill)

    # Check credits (100 per search, same as seller)
    SEARCH_COST = 100
    current_credits = int(user.get('credits') or 0)
    if current_credits < SEARCH_COST:
        flash(f'Insufficient credits. You need {SEARCH_COST} credits but have only {current_credits}. Please top up.', 'error')
        return render_form(prefill)

    # Deduct credits
    new_balance = current_credits - SEARCH_COST
    supabase.table('users').update({'credits': new_balance}).eq('id', user_id).execute()

    # Log transaction
    supabase.table('transactions').insert({
        'user_id': user_id,
        'type': 'buyer_search',
        'amount': -SEARCH_COST,
        'balance_after': new_balance,
        'description': f'Buyer search: {make} {model} {variant} ({year})',
    }).execute()

    # Build query string for buyer dashboard
    params = {
        'make': make, 'fuel': fuel, 'model': model, 'variant': variant,
        'year': year, 'condition': condition,
    }
    if asking_price is not None:
        params['asking_price'] = asking_price

    return redirect(url_for('buyer_dashboard', **params))
