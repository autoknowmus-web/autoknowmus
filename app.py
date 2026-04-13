from flask import Flask, render_template, request

app = Flask(__name__)

# Mock data for the landing page stats
stats = {
    "total_searches": "1,24,500+",
    "real_transactions": "8,432"
}

@app.route('/')
def index():
    return render_template('index.html', stats=stats)

@app.route('/seller')
def seller():
    # This renders the page where the user enters car details
    return render_template('seller.html')

@app.route('/get_insights', methods=['POST'])
def get_insights():
    # This catches the data from the 'Get Selling Insights' button
    # For now, we'll send them to a success page or dashboard
    # Later, we can add your model.py logic here to show real prices!
    return render_template('dashboard.html')

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)