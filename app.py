# [UPDATE] Update this route to use your existing 'dashboard.html'
@app.route('/generate_report', methods=['POST'])
def generate_report():
    current_credits = session.get('credits', 0)
    
    if current_credits >= 100:
        session['credits'] = current_credits - 100
        
        # Store the search details to display on the dashboard
        session['last_search'] = {
            'make': request.form.get('make'),
            'model': request.form.get('model'),
            'variant': request.form.get('variant'),
            'year': request.form.get('year'),
            'city': request.form.get('city')
        }
        
        # [FIX] Redirects to the 'dashboard' route
        return redirect(url_for('dashboard'))
    else:
        flash("Insufficient credits! Please purchase more.")
        return redirect(url_for('role'))

@app.route('/dashboard')
def dashboard():
    if 'user_name' not in session:
        return redirect(url_for('index'))
    
    # Retrieve the vehicle data we just submitted
    search_data = session.get('last_search', {})
    return render_template('dashboard.html', data=search_data)
