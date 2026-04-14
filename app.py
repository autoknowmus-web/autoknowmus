from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/role', methods=['GET', 'POST'])
def role():
    # This matches the 'Continue to Selection' button on your home page
    return render_template('role.html')

@app.route('/seller')
def seller():
    return render_template('seller.html')

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

if __name__ == '__main__':
    app.run(debug=True)