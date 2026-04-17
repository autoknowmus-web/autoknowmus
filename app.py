# [UPDATE] Update this specific route in your app.py
@app.route('/generate_report', methods=['POST'])
def generate_report():
    current_credits = session.get('credits', 0)
    
    if current_credits >= 100:
        session['credits'] = current_credits - 100
        # flash("Intelligence Report Generated! 100 Credits deducted.") # Optional: Remove if dashboard explains it
        
        # Capture form data to show on the dashboard
        session['last_search'] = {
            'make': request.form.get('make'),
            'model': request.form.get('model'),
            'variant': request.form.get('variant'),
            'year': request.form.get('year')
        }
        
        # [FIX] Redirect to the actual results dashboard
        return redirect(url_for('seller_dashboard'))
    else:
        flash("Insufficient credits! Please purchase more.")
        return redirect(url_for('role'))

@app.route('/seller_dashboard')
def seller_dashboard():
    if 'user_name' not in session:
        return redirect(url_for('index'))
    
    search_data = session.get('last_search', {})
    return render_template('seller_dashboard.html', data=search_data)
