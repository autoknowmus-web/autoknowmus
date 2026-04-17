@app.route('/auth')
def auth():
    token = google.authorize_access_token()
    user_info = google.get('userinfo').json()
    
    # Store user info in session
    session['user_id'] = user_info['id']
    session['user_name'] = user_info['name']
    
    # [Requirement Fix] Check if first time login to give 300 Bonus Credits
    # In a real app, you'd check a database. Here we use session for MVP:
    if 'has_logged_in' not in session:
        session['credits'] = 300  # 3 free searches (100 each)
        session['has_logged_in'] = True
    
    return redirect(url_for('role'))

@app.route('/role')
def role():
    # Ensure credits are visible globally
    credits = session.get('credits', 0)
    name = session.get('user_name', 'Guest')
    return render_template('role.html', user_name=name, credits=credits)
