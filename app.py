from flask import Flask, render_template

app = Flask(__name__)

# Home Page
@app.route('/')
def index():
    try:
        return render_template('index.html')
    except Exception as e:
        return f"Error loading index: {str(e)}"

# Role Selection
@app.route('/role', methods=['GET', 'POST'])
def role():
    return render_template('role.html')

# Seller Page
@app.route('/seller')
def seller():
    return render_template('seller.html')

# Dashboard Page
@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

if __name__ == '__main__':
    app.run(debug=True)
